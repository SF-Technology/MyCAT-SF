package org.opencloudb.mysql.nio.handler;

import java.util.concurrent.locks.ReentrantLock;

import org.opencloudb.backend.BackendConnection;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.server.NonBlockingSession;

/**
 * @author 01140003
 * 用来处理下发到多个全局表分片的查询操作
 * （select for update和select lock in share mode需要下发到全局表的所有分片）
 */
public class GlobalTableMultiNodeQueryHandler extends MultiNodeQueryHandler{

	private final RouteResultset rrs;
	private volatile BackendConnection backConn; // 只处理来自此连接的结果集，确保只处理一个分片的结果集
	private final ReentrantLock lock;
	private int nodeCount;
	
	public GlobalTableMultiNodeQueryHandler(int sqlType, RouteResultset rrs, boolean autocommit,
			NonBlockingSession session) {
		super(sqlType, rrs, autocommit, session);
		this.rrs = rrs;
		
		this.lock = new ReentrantLock();
	}
	
	@Override
	public void execute() throws Exception {
		this.backConn = null; 
		this.nodeCount = rrs.getNodes().length;
		super.execute();
	}

	@Override
	public void rowEofResponse(final byte[] eof, BackendConnection conn) {
		if (countResultsNotReturned()){ // 如果收到了所有分片的结果集，则重置backConn
			this.backConn = null;
		}

		super.rowEofResponse(eof, conn);
	}
	
	@Override
	public void rowResponse(final byte[] row, final BackendConnection conn) {
		lock.lock();
		try {
			if(backConn == null){ // 将backConn标记为第一个返回结果集的连接
				backConn = conn;
			}
					
			if(!conn.equals(backConn)){ // 如果backConn已经被其它连接标记，则不对当前连接返回的结果集进行处理
				return;
			}
		} finally {
			lock.unlock();
		}
		
		super.rowResponse(row, conn);
	}
	
	/**
	 * 对返回的结果集数量进行计数，如果成功从一个分片收到结果集，nodeCount就减1
	 * @return 是否收到所有的结果集
	 */
	private boolean countResultsNotReturned() {
		boolean zeroReached = false;
		lock.lock();
		try {
			zeroReached = --nodeCount == 0;
		} finally {
			lock.unlock();
		}
		
		return zeroReached;
	}

}
