package org.opencloudb.manager.handler;

import org.apache.log4j.Logger;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.MycatManageStatementParser;
import org.opencloudb.manager.parser.druid.statement.MycatListStatement;
import org.opencloudb.manager.response.ListDataHosts;
import org.opencloudb.manager.response.ListDataNodes;
import org.opencloudb.manager.response.ListFunctions;
import org.opencloudb.manager.response.ListRules;
import org.opencloudb.manager.response.ListSchemas;
import org.opencloudb.manager.response.ListSqlwallVariables;
import org.opencloudb.manager.response.ListSystemVariables;
import org.opencloudb.manager.response.ListTables;
import org.opencloudb.manager.response.ListUsers;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.parser.ParserException;

/**
 * @author CrazyPig
 * @since 2017-02-08
 *
 */
public class MycatConfigListHandler {
	
	private static final Logger LOGGER = Logger.getLogger(MycatConfigListHandler.class);
	
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
					handleListRules(c);
					break;
				case FUNCTIONS:
					handleListFunctions(c);
					break;
				case USERS:
					handleListUsers(c);
					break;
				case SYSTEM_VARIABLES:
					handleListSystemVariables(c);
					break;
				case SQLWALL_VARIABLES:
					hamdleListSqlwallVariables(c);
					break;
				default:
					c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "Unsupport statment : " + sql);
					break;
				}
			} else {
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "Unsupport statment : " + sql);
			}
		} catch(ParserException e) {
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
		} catch(Exception e) {
			LOGGER.error(e.getMessage(), e);
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
	
	public static void handleListUsers(ManagerConnection c) {
		ListUsers.response(c);
	}
	
	public static void handleListRules(ManagerConnection c) {
		ListRules.response(c);
	}
	
	public static void handleListFunctions(ManagerConnection c) {
		ListFunctions.response(c);
	}
	
	public static void handleListSystemVariables(ManagerConnection c) {
		ListSystemVariables.response(c);
	}

	public static void hamdleListSqlwallVariables(ManagerConnection c) {
		ListSqlwallVariables.response(c);
	}
}
