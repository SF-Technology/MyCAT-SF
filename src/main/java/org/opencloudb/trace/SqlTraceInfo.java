package org.opencloudb.trace;

import org.opencloudb.backend.BackendConnection;
import org.opencloudb.mysql.nio.MySQLConnection;
import org.opencloudb.net.AbstractConnection;
import org.opencloudb.server.ServerConnection;

/**
 * SQL 执行结果
 * 
 * @author zhuam
 *
 */
public class SqlTraceInfo {
	private long id;
	private String mess ;
	private String threadInfo;
	private String autocommit;
	private String connection;


	public SqlTraceInfo(AbstractConnection connection) {
		mess = "Front Connection info : " ;
		this.threadInfo = Thread.currentThread().getName();
		this.connection = connection == null ? "" : connection.toString();
		if(connection instanceof ServerConnection){
			ServerConnection serverConnection = ((ServerConnection) connection);
			id = serverConnection.getId();
		}
	}

	public SqlTraceInfo(BackendConnection connection) {
		mess = "Backend Connection info : " ;
		this.threadInfo = Thread.currentThread().getName();
		this.connection = connection == null ? "" : connection.toString();
		if(connection instanceof MySQLConnection){
			MySQLConnection mySQLConnection = ((MySQLConnection) connection);
			id = mySQLConnection.getId();

		}
	}

	@Override
	public String toString() {
		return mess + " [Current thread :" + threadInfo + ",connection info ={"
				+ connection + "}]";
	}
}
