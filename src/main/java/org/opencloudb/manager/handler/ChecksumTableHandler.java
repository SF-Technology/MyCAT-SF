package org.opencloudb.manager.handler;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.Fields;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.FrontendConnection;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.sqlengine.AllJobFinishedListener;
import org.opencloudb.sqlengine.EngineCtx;
import org.opencloudb.sqlengine.SQLJobHandler;
import org.opencloudb.util.StringUtil;

import com.google.common.base.Joiner;

/**
 * 校验全局表数据一致性逻辑处理类
 * @author CrazyPig
 * @since 2017-01-13
 *
 */
public class ChecksumTableHandler implements AllJobFinishedListener {
	
	private static final int FIELD_COUNT = 2;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        fields[i] = PacketUtil.getField("Table", Fields.FIELD_TYPE_VAR_STRING);
        fields[i].packetId = ++packetId;
        i++;
        fields[i] = PacketUtil.getField("Checksum", Fields.FIELD_TYPE_VAR_STRING);
        fields[i].packetId = ++packetId;
        eof.packetId = ++packetId;
    }
	
    private String schemaName;
    private TableConfig gTableConf;
//    private GlobalTableLockHolder tableLockHolder;
	private FrontendConnection frontendConn;
	private Map<String, String> checksumMap = new ConcurrentHashMap<String, String>();
	private EngineCtx engineCtx;
	private AtomicInteger jobCount;
	private volatile boolean fail = false;
	
	private SQLJobHandler sqlJobHandler = new SQLJobHandler() {
		
//		得到的结果:
//		+-----------------+------------+
//		| Table           | Checksum   |
//		+-----------------+------------+
//		| test.tb_test_cs | 4208630266 |
//		+-----------------+------------+
//		1 row in set (1.03 sec)
		
		@Override
		public boolean onRowData(String dataNode, byte[] rowData) {
			// sql下发后, 接收后端MySQL数据
			RowDataPacket rowPacket = new RowDataPacket(2);
			rowPacket.read(rowData);
			// 获取checksum字段值
			String checksumVal = null;
			byte[] fieldValue = rowPacket.fieldValues.get(1);
			if(fieldValue != null) {
				checksumVal = new String(fieldValue);
			} else {
				// ConcurrentHashMap can not put null, will throw NPE!
				checksumVal = "NULL";
			}
			checksumMap.put(dataNode, checksumVal);
			return false;
		}
		
		@Override
		public void onHeader(String dataNode, byte[] header, List<byte[]> fields) {
			
		}
		
		@Override
		public void finished(String dataNode, boolean failed) {
			if(failed) {
				fail = true;
			}
			if(jobCount.decrementAndGet() <= 0) {
				// 标志所有异步任务已经完成
				engineCtx.endJobInput();
			}
		}
	};
	
	/**
	 * checksum table(全局表数据一致性校验)逻辑入口
	 * @param schemaName
	 * @param gTableConf
	 * @param c
	 */
	public void handle(String schemaName, TableConfig gTableConf, FrontendConnection c) {
		
		this.schemaName = schemaName;
		this.gTableConf = gTableConf;
		this.frontendConn = c;
		int dnSize = gTableConf.getDataNodes().size();
		
		if(dnSize == 1) {
			// 全局表只有一个节点?不浪费资源校验
			response(true, "only one datanode, needn't check");
			return ;
		}
		
		this.jobCount = new AtomicInteger(dnSize);
		this.engineCtx = new EngineCtx(null);
		
		String[] dataNodes = new String[dnSize];
		gTableConf.getDataNodes().toArray(dataNodes);
		engineCtx.setAllJobFinishedListener(this);
		
		String sql = "checksum table " + gTableConf.getName();
		// 设置isChecksuming标志位为true, 标识checksum的开始
//		tableLockHolder = MycatServer.getInstance().getTableLockManager().getGlobalTableLockHolder(schemaName, gTableConf.getName());
//		tableLockHolder.startChecksum();
		// 并发执行 [Warning]:并发执行会占用业务线程池
		engineCtx.executeNativeSQLParallJob(dataNodes, sql, sqlJobHandler);
		
	}

	@Override
	public void onAllJobFinished(EngineCtx ctx) {
		
		// 设置isChecksuming为false, 并唤醒其他阻塞在全局表上的线程
//		tableLockHolder.endChecksum();
		
		// 异常处理
		if(fail) {
			// checksum执行异常, 返回错误信息
			frontendConn.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, 
					"There is something wrong when executing checksum command from backend, please make sure backend connection is OK and try again!");
			return ;
		}
		
		// 对checksum结果进行校验
		Map<String, List<String>> checksumValMap = new HashMap<String, List<String>>();
		for(String dataNode : this.checksumMap.keySet()) {
			String checksumVal = this.checksumMap.get(dataNode);
			if(checksumValMap.get(checksumVal) == null) {
				List<String> dnList = new ArrayList<String>();
				dnList.add(dataNode);
				checksumValMap.put(checksumVal, dnList);
			} else {
				checksumValMap.get(checksumVal).add(dataNode);
			}
		}
		
		// 返回校验结果
		boolean isOk = checksumValMap.size() == 1;
		if(isOk) {
			String onlyOne = checksumValMap.keySet().iterator().next();
			response(isOk, "[" + onlyOne + "]");
		} else {
			StringBuilder sb = new StringBuilder();
			for(String checksumVal : checksumValMap.keySet()) {
				sb.append("[" + checksumVal + "] -> {" + Joiner.on(",").join(checksumValMap.get(checksumVal)) + "};");
			}
			response(isOk, sb.toString());
		}
		
	}
	
	private void response(boolean isOk, String detail) {
		
		FrontendConnection c = this.frontendConn;
		ByteBuffer buffer = c.allocate();

        // write header
        buffer = header.write(buffer, c,true);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, c,true);
        }

        // write eof
        buffer = eof.write(buffer, c,true);

        // write rows
        byte packetId = eof.packetId;
        RowDataPacket rowPacket = new RowDataPacket(FIELD_COUNT);
        rowPacket.packetId = ++packetId;
    	rowPacket.add((schemaName + "." + gTableConf.getName().toLowerCase()).getBytes());
    	
        if(isOk) {
        	// 一致
        	if(StringUtil.isEmpty(detail)) {
        		rowPacket.add("OK".getBytes());
        	} else {
        		rowPacket.add(("OK, " + detail).getBytes());
        	}
        } else {
        	// 不一致
        	if(StringUtil.isEmpty(detail)) {
        		rowPacket.add("Not OK".getBytes());
        	} else {
        		rowPacket.add(("Not OK, " + detail).getBytes());
        	}
        }
        
        buffer = rowPacket.write(buffer, c, true);
        
        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c,true);

        // post write
        c.write(buffer);
	}
	
}
