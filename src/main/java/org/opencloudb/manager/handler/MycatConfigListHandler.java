package org.opencloudb.manager.handler;

import org.opencloudb.config.ErrorCode;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.MycatManageStatementParser;
import org.opencloudb.manager.parser.druid.statement.MycatListStatement;
import org.opencloudb.manager.response.ListDataHosts;
import org.opencloudb.manager.response.ListDataNodes;
import org.opencloudb.manager.response.ListSchemas;
import org.opencloudb.manager.response.ListTables;

import com.alibaba.druid.sql.ast.SQLStatement;

/**
 * @author CrazyPig
 * @since 2017-02-08
 *
 */
public class MycatConfigListHandler {
	
	public static void handle(String sql, ManagerConnection c) {
		
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		try {
			SQLStatement stmt = parser.parseStatement();
			if(stmt instanceof MycatListStatement) {
				MycatListStatement listStmt = (MycatListStatement) stmt;
				switch(listStmt.getTarget()) {
				case SCHEMAS:
					handleListSchemas(c);
					break;
				case TABLES:
					handleListTables(c);
					break;
				case DATANODES:
					handleListDataNodes(c);
					break;
				case DATAHOSTS:
					handleListDataHost(c);
					break;
				case RULES:
					// TODO
					break;
				case FUNCTIONS:
					// TODO
					break;
				case USERS:
					// TODO
					break;
				default:
					break;
				}
			} else {
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "Unsupport statment : " + sql);
			}
		} catch(Exception e) {
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
		}
		
	}
	
	public static void handleListSchemas(ManagerConnection c) {
		ListSchemas.response(c);
	}
	
	public static void handleListTables(ManagerConnection c) {
		ListTables.response(c);
	}
	
	public static void handleListDataNodes(ManagerConnection c) {
		ListDataNodes.response(c);
	}
	
	public static void handleListDataHost(ManagerConnection c) {
		ListDataHosts.response(c);
	}

}
