package org.opencloudb.manager.handler;

import org.apache.log4j.Logger;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.MycatManageStatementParser;
import org.opencloudb.manager.parser.druid.statement.MycatDropDataHostStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropDataNodeStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropFunctionStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropMapFileStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropProcedureStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropRuleStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropSchemaStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropTableStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropUserStatement;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.parser.ParserException;

/**
 * 
 * @author CrazyPig
 * @since 2017-02-21
 *
 */
public class MycatConfigDropHandler {
	
	private static final Logger LOGGER = Logger.getLogger(MycatConfigDropHandler.class);
	
	public static void handle(String sql, ManagerConnection c) {
		
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		try {
			SQLStatement stmt = parser.parseStatement();
			if(stmt instanceof MycatDropSchemaStatement) {
				DropSchemaHandler.handle(c, (MycatDropSchemaStatement) stmt, sql);
			} else if(stmt instanceof MycatDropTableStatement) {
				DropTableHandler.handle(c, (MycatDropTableStatement) stmt, sql);
			} else if(stmt instanceof MycatDropDataNodeStatement) {
				DropDataNodeHandler.handle(c, (MycatDropDataNodeStatement) stmt, sql);;
			} else if(stmt instanceof MycatDropDataHostStatement) {
				DropDataHostHandler.handle(c, (MycatDropDataHostStatement) stmt, sql);
			} else if(stmt instanceof MycatDropUserStatement) {
				DropUserHandler.handle(c, (MycatDropUserStatement) stmt, sql);
			} else if(stmt instanceof MycatDropRuleStatement){
				DropRuleHandler.handle(c, (MycatDropRuleStatement) stmt, sql);
			} else if(stmt instanceof MycatDropFunctionStatement){
				DropFunctionHandler.handle(c, (MycatDropFunctionStatement)stmt, sql);
			} else if(stmt instanceof MycatDropMapFileStatement){
				DropMapFileHandler.handle(c, (MycatDropMapFileStatement)stmt, sql);
			} else if (stmt instanceof MycatDropProcedureStatement) {
			    DropProcedureHandler.handle(c, (MycatDropProcedureStatement) stmt, sql);
			} else { // TODO more... 
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "Unsupport statement : " + sql);
			}
		} catch(ParserException e) {
			c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, e.getMessage());
		} catch(Exception e) {
			LOGGER.error(e.getMessage(), e);
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
		}
	}

}
