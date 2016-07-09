package org.opencloudb.mysql.nio.handler;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.NonBlockingSession;
import org.opencloudb.server.parser.ServerParse;

public class UnLockTablesHandler extends MultiNodeHandler implements ResponseHandler {

	private static final Logger LOGGER = Logger.getLogger(UnLockTablesHandler.class);

	private final NonBlockingSession session;
	private final boolean autocommit;
	private final String srcStatement;

	public UnLockTablesHandler(NonBlockingSession session, boolean autocommit, String sql) {
		super(session);
		this.session = session;
		this.autocommit = autocommit;
		this.srcStatement = sql;
	}

	public void execute() {
		ConcurrentHashMap<String, BackendConnection> lockedConns = session.getLockedTarget();
		Set<String> dnSet = lockedConns.keySet();
		this.reset(lockedConns.size());
		for (String dataNode : dnSet) {
			RouteResultsetNode node = new RouteResultsetNode(dataNode, ServerParse.UNLOCK, srcStatement);
			BackendConnection conn = lockedConns.get(dataNode);
			if (clearIfSessionClosed(session)) {
				return;
			}
			conn.setResponseHandler(this);
			try {
				conn.execute(node, session.getSource(), autocommit);
			} catch (Exception e) {
				connectionError(e, conn);
			}
		}
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		super.connectionError(e, conn);
	}

	@Override
	public void connectionAcquired(BackendConnection conn) {
		LOGGER.error("unexpected invocation: connectionAcquired from unlock tables");
	}

	@Override
	public void errorResponse(byte[] err, BackendConnection conn) {
		super.errorResponse(err, conn);
	}

	@Override
	public void okResponse(byte[] data, BackendConnection conn) {
		boolean executeResponse = conn.syncAndExcute();
		if (executeResponse) {
			if (clearIfSessionClosed(session)) {
                return;
            } else if (canClose(conn, false)) {
                return;
            }
			boolean isEndPack = decrementCountBy(1);
			if (isEndPack) {
				if (this.isFail() || session.closed()) {
					tryErrorFinished(true);
					return;
				}
				OkPacket ok = new OkPacket();
				ok.read(data);
				lock.lock();
				try {
					ok.packetId = ++ packetId;
					ok.serverStatus = session.getSource().isAutocommit() ? 2:1;
				} finally {
					lock.unlock();
				}
				ok.write(session.getSource());
			}
		}
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof, BackendConnection conn) {
		LOGGER.error(new StringBuilder().append("unexpected packet for ")
				.append(conn).append(" bound by ").append(session.getSource())
				.append(": field's eof").toString());
	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
		LOGGER.warn(new StringBuilder().append("unexpected packet for ")
				.append(conn).append(" bound by ").append(session.getSource())
				.append(": row data packet").toString());
	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
		LOGGER.error(new StringBuilder().append("unexpected packet for ")
				.append(conn).append(" bound by ").append(session.getSource())
				.append(": row's eof").toString());
	}

	@Override
	public void writeQueueAvailable() {
		// TODO Auto-generated method stub

	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		// TODO Auto-generated method stub

	}

}
