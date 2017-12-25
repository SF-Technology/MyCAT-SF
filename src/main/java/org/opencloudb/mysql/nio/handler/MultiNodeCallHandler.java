package org.opencloudb.mysql.nio.handler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.mysql.nio.handler.MultiNodeCallHandler.MultiResultSet.Resultset;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.NonBlockingSession;
import org.opencloudb.server.ServerConnection;

/**
 * 多节点存储过程调用处理器
 * @author CrazyPig
 * @since 1.5.3
 *
 */
public class MultiNodeCallHandler extends MultiNodeHandler {
    
    private static final Logger LOGGER = Logger.getLogger(MultiNodeCallHandler.class);
    
    private final RouteResultset rrs;
    private final boolean autocommit;
    private final Map<String, MultiResultSet> dn2RsMap;
    
    private int okCount;
    
    public MultiNodeCallHandler(RouteResultset rrs, NonBlockingSession session) {
        super(session);
        this.rrs = rrs;
        this.autocommit = session.getSource().isAutocommit();
        dn2RsMap = new HashMap<String, MultiResultSet>();
        for (RouteResultsetNode node : rrs.getNodes()) {
            dn2RsMap.put(node.getName(), new MultiResultSet());
        }
    }
    
    protected void reset(int initCount) {
        super.reset(initCount);
        this.okCount = initCount;
    }
    
    public void execute() throws Exception {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            this.reset(rrs.getNodes().length);
        } finally {
            lock.unlock();
        }
        MycatConfig conf = MycatServer.getInstance().getConfig();
        for (final RouteResultsetNode node : rrs.getNodes()) {
            BackendConnection conn = session.getTarget(node);
            if (session.tryExistsCon(conn, node)) {
                _execute(conn, node);
            } else {
                // create new connection
                PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
                dn.getConnection(dn.getDatabase(), autocommit, node, this, node);
            }
        }
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        final RouteResultsetNode node = (RouteResultsetNode) conn
                .getAttachment();
        session.bindConnection(node, conn);
        _execute(conn, node);
    }
    
    /**
     *  对于存储过程, 其比较特殊, 查询结果返回rowEof报文以后, 还会再返回一个OK报文, 才算结束
     */
    @Override
    public void okResponse(byte[] data, BackendConnection conn) {
        boolean executeResponse = conn.syncAndExcute();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("received ok response ,executeResponse:"
                    + executeResponse + " from " + conn);
        }
        if (executeResponse) {
            // 是否为最后一个OK包
            boolean isEndPacket = decrementOkCountBy(1);
            if (isEndPacket) {
                if (this.autocommit && !session.getSource().isLocked()) {// clear all connections
                    session.releaseConnections(false);
                }
                if (this.isFail() || session.closed()) {
                    tryErrorFinished(true);
                    return;
                }
                // 这里就可以将整个结果集写回给客户端
                lock.lock();
                try {
                    writeMultiResultset(data);
                } catch (Exception e) {
                    handleDataProcessException(e);
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof,
            BackendConnection conn) {
        try {
            RouteResultsetNode rrsNode = (RouteResultsetNode) conn.getAttachment();
            String dataNode = rrsNode.getName();
            MultiResultSet multiRs = dn2RsMap.get(dataNode);
            multiRs.newResultset();
            multiRs.append(header, fields, eof);
        } catch (Throwable e) {
            handleDataProcessException(e);
        }
    }
    
    public void handleDataProcessException(Throwable e) {
        if (!errorRepsponsed.get()) {
            this.error = e.toString();
            LOGGER.warn("caught exception ", e);
            setFail(e.toString());
            this.tryErrorFinished(true);
        }
    }

    @Override
    public void rowResponse(byte[] row, BackendConnection conn) {
        try {
            RouteResultsetNode rrsNode = (RouteResultsetNode) conn.getAttachment();
            String dataNode = rrsNode.getName();
            MultiResultSet multiRs = dn2RsMap.get(dataNode);
            multiRs.addRow(row);
        } catch (Throwable e) {
            handleDataProcessException(e);
        }
    }

    @Override
    public void rowEofResponse(byte[] eof, BackendConnection conn) {
        try {
            RouteResultsetNode rrsNode = (RouteResultsetNode) conn.getAttachment();
            String dataNode = rrsNode.getName();
            MultiResultSet multiRs = dn2RsMap.get(dataNode);
            multiRs.endResultset(eof);
        } catch (Throwable e) {
            handleDataProcessException(e);
        }
    }

    @Override
    public void writeQueueAvailable() {
        
    }
    
    @Override
    public void clearResources() {
        for (String dataNode : dn2RsMap.keySet()) {
            dn2RsMap.get(dataNode).clear();
        }
        dn2RsMap.clear();
    }
    
    private void _execute(BackendConnection conn, RouteResultsetNode node) {
        if (clearIfSessionClosed(session)) {
            return;
        }
        conn.setResponseHandler(this);
        try {
            conn.execute(node, session.getSource(), autocommit);
        } catch (IOException e) {
            connectionError(e, conn);
        }
    }
    
    private boolean decrementOkCountBy(int finished) {
        lock.lock();
        try {
            return --okCount == 0;
        } finally {
            lock.unlock();
        }
    }
    
    private void writeMultiResultset(byte[] ok) {
        ServerConnection source = session.getSource();
        ByteBuffer buffer = source.allocate();
        List<String> dataNodes = new ArrayList<String>(new TreeSet<String>(dn2RsMap.keySet()));
        String firstDn = dataNodes.get(0);
        MultiResultSet multiRs = dn2RsMap.get(firstDn);
        int rsCount = multiRs.getResultsetSize();
        if (rsCount > 0) {
            for (int i = 0; i < rsCount; i++) {
                Resultset rs = multiRs.getResultset(i);
                byte[] fieldHeader = rs.fieldHeader;
                fieldHeader[3] = ++packetId;
                // print(fieldHeader);
                // write field count packet
                buffer = source.writeToBuffer(fieldHeader, buffer);
                List<byte[]> fields = rs.fields;
                // write field packets
                for (byte[] field : fields) {
                    field[3] = ++packetId;
                    // print(field);
                    buffer = source.writeToBuffer(field, buffer);
                }
                // write field eof packet
                byte[] fieldEof = rs.fieldEof;
                fieldEof[3] = ++packetId;
                // print(fieldEof);
                buffer = source.writeToBuffer(fieldEof, buffer);
                
                // write row packets
                for (byte[] row : rs.rows) {
                    row[3] = ++packetId;
                    // print(row);
                    buffer = source.writeToBuffer(row, buffer);
                }
                // write other dataNode resultset row packets
                for (int j = 1, dnSize = dataNodes.size(); j < dnSize; j++) {
                    String otherDn = dataNodes.get(j);
                    MultiResultSet multiRs1 = dn2RsMap.get(otherDn);
                    Resultset rs1 = multiRs1.getResultset(i);
                    for (byte[] row : rs1.rows) {
                        row[3] = ++packetId;
                        // print(row);
                        buffer = source.writeToBuffer(row, buffer);
                    }
                }
                // write row eof packet
                byte[] rowEof = rs.rowEof;
                rowEof[3] = ++packetId;
                buffer = source.writeToBuffer(rowEof, buffer);
            }
        }
        
        ok[3] = ++packetId;
        // print(ok);
        buffer = source.writeToBuffer(ok, buffer);
        source.write(buffer);
    }
    
//    private void print(byte[] bytes) {
//        StringBuilder sb = new StringBuilder();
//        for (byte b : bytes) {
//            sb.append(String.format("%02X ", b));
//        }
//        System.out.println(sb.toString());
//    }
    
    /**
     * 封装多结果集
     * @author CrazyPig
     * @since 1.5.3
     *
     */
    static class MultiResultSet {
        
        static class Resultset {
            
            /** field count packet 数据内容 **/
            byte[] fieldHeader;
            /** field packets 数据内容 **/
            List<byte[]> fields;
            /** field eof packet 数据内容 **/
            byte[] fieldEof;
            /** row packets 数据内容 **/
            List<byte[]> rows;
            /** row eof packet 数据内容 **/
            byte[] rowEof;
            
            public Resultset() {
                rows = new ArrayList<byte[]>();
            }
            
            public void clear() {
                fields.clear();
                rows.clear();
                fieldHeader = null;
                fields = null;
                fieldEof = null;
                rows = null;
                rowEof = null;
            }
            
        }
        
        /** 代表resultset个数 **/
        int rsCount;
        /** resultset数据 **/
        List<Resultset> rsList;
        
        public MultiResultSet() {
            rsCount = 0;
            rsList = new ArrayList<Resultset>();
        }
        
        public int getResultsetSize() {
            return rsCount;
        }
        
        public Resultset getResultset(int index) {
            if (index < 0 || index >= rsCount) {
                throw new IndexOutOfBoundsException("index : " + index + " not inbound between 0 and " + rsCount);
            }
            return rsList.get(index);
        }
        
        public void newResultset() {
            rsList.add(new Resultset());
            rsCount++;
        }
        
        public void append(byte[] header, List<byte[]> fields, byte[] eof) {
            Resultset rs = rsList.get(rsCount - 1);
            rs.fieldHeader = header;
            rs.fields = fields;
            rs.fieldEof = eof;
        }
        
        public void addRow(byte[] row) {
            Resultset rs = rsList.get(rsCount - 1);
            rs.rows.add(row);
        }
        
        public void endResultset(byte[] rowEof) {
            Resultset rs = rsList.get(rsCount - 1);
            rs.rowEof = rowEof;
        }
        
        public void clear() {
            for (Resultset rs : rsList) {
                rs.clear();
            }
            rsList.clear();
            rsList = null;
        }
        
    }
    
}
