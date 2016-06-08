package org.opencloudb.mysql.nio.handler;

import java.util.List;

import org.opencloudb.backend.BackendConnection;

public class LockTablesHandler implements ResponseHandler {
	
	public void lockTables() {
		
	}
	
	public void unlockTables() {
		
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		// TODO Auto-generated method stub

	}

	@Override
	public void connectionAcquired(BackendConnection conn) {
		// TODO Auto-generated method stub

	}

	@Override
	public void errorResponse(byte[] err, BackendConnection conn) {
		// TODO Auto-generated method stub

	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		// TODO Auto-generated method stub

	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof, BackendConnection conn) {
		// TODO Auto-generated method stub

	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
		// TODO Auto-generated method stub

	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
		// TODO Auto-generated method stub

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
