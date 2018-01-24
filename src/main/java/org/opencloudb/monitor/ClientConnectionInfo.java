package org.opencloudb.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author zagnix
 * @create 2016-11-02 19:00
 */

public class ClientConnectionInfo {

    private final static Logger LOGGER =
            LoggerFactory.getLogger(ClientConnectionInfo.class);
    private String processor;
    private long id;
    private String host;
    private int port;
    private int localPort;
    private String user;
    private String schema;
    private String charset;
    private long netIn;
    private long netOut;
    private long aliveTime;
    private long recvBuffer;
    private long sendQueue;
    private String txLevel;
    private String autoCommit;



    public String getProcessor() {
        return processor;
    }

    public void setProcessor(String processor) {
        this.processor = processor;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getLocalPort() {
        return localPort;
    }

    public void setLocalPort(int localPort) {
        this.localPort = localPort;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public long getNetIn() {
        return netIn;
    }

    public void setNetIn(long netIn) {
        this.netIn = netIn;
    }

    public long getNetOut() {
        return netOut;
    }

    public void setNetOut(long netOut) {
        this.netOut = netOut;
    }

    public long getAliveTime() {
        return aliveTime;
    }

    public void setAliveTime(long aliveTime) {
        this.aliveTime = aliveTime;
    }

    public long getRecvBuffer() {
        return recvBuffer;
    }

    public void setRecvBuffer(long recvBuffer) {
        this.recvBuffer = recvBuffer;
    }

    public long getSendQueue() {
        return sendQueue;
    }

    public void setSendQueue(long sendQueue) {
        this.sendQueue = sendQueue;
    }

    public String getTxLevel() {
        return txLevel;
    }

    public void setTxLevel(String txLevel) {
        this.txLevel = txLevel;
    }

    public String getAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(String autoCommit) {
        this.autoCommit = autoCommit;
    }

    @Override
    public String toString() {
        return "ClientConnectionInfo{" +
                "processor='" + processor + '\'' +
                ", id=" + id +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", localPort=" + localPort +
                ", user='" + user + '\'' +
                ", schema='" + schema + '\'' +
                ", charset='" + charset + '\'' +
                ", netIn=" + netIn +
                ", netOut=" + netOut +
                ", aliveTime=" + aliveTime +
                ", recvBuffer=" + recvBuffer +
                ", sendQueue=" + sendQueue +
                ", txLevel='" + txLevel + '\'' +
                ", autoCommit='" + autoCommit + '\'' +
                '}';
    }



    public void update() {

        /**
         * 1.查询是已经存在
         */
        boolean isAdd = true;
        final Connection h2DBConn =
                H2DBMonitorManager.getH2DBMonitorManager().getH2DBMonitorConn();
        Statement stmt = null;
        ResultSet rset = null;

        try {
            String sql = "select id from t_connection_cli where id = " + getId();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("sql === >  " + sql);
            }
            stmt = h2DBConn.createStatement();
            rset = stmt.executeQuery(sql);
            if (rset.next()){
                isAdd = false;
            }
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
        }finally {
            try {
                if(stmt !=null){
                    stmt.close();
                }
                if (rset !=null){
                    rset.close();
                }
            } catch (SQLException e) {
                LOGGER.error(e.getMessage());
            }
        }

        /**
         * 2.根据1决定是添加还是更新
         */
        String sql = null;

        if(isAdd){
            sql = "INSERT INTO t_connection_cli VALUES(" + getId()+ ",'"
                    + getProcessor() + "','"
                    + getHost() + "',"
                    + getPort() + ","
                    + getLocalPort() + ",'"
                    + getUser() +"','"
                    + getSchema() +"','"
                    + getCharset() + "',"
                    + getNetIn() + ","
                    + getNetOut() + ","
                    + getAliveTime() + ","
                    + getRecvBuffer() + ","
                    + getSendQueue() + ",'"
                    + getTxLevel() + "','"
                    + getAutoCommit() + "')";

        }else {

            sql = "UPDATE t_connection_cli SET processor ='" + getProcessor() +"'," +
                    "host = '" + getHost() + "',"  +
                    "port = " + getPort() + ","  +
                    "l_port = " + getLocalPort()+ "," +
                    "user = '" + getUser() + "'," +
                    "schema = '" + getSchema() + "'," +
                    "charset = '" + getCharset() + "'," +
                    "net_in = " + getNetIn() + "," +
                    "net_out = " + getNetOut() + "," +
                    "alive_time = " + getAliveTime() + "," +
                    "recv_buffer = " + getRecvBuffer() + "," +
                    "send_queue = " + getSendQueue() + "," +
                    "txlevel = '" + getTxLevel() + "'," +
                    "autocommit = '" + getAutoCommit() + "'" +
                    " WHERE id = " + getId();
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("sql === >  " + sql);
        }

        try {
            stmt = h2DBConn.createStatement();
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
        }finally {
            try {
                if(stmt !=null){
                    stmt.close();
                }
                if (rset !=null){
                    rset.close();
                }
            } catch (SQLException e) {
                LOGGER.error(e.getMessage());
            }
        }
    }
}

