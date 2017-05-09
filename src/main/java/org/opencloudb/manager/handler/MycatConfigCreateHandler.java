package org.opencloudb.manager.handler;

import org.apache.log4j.Logger;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.MycatManageStatementParser;
import org.opencloudb.manager.parser.druid.statement.MycatCreateChildTableStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateDataHostStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateDataNodeStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateFunctionStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateMapFileStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateRuleStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateSchemaStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateTableStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateUserStatement;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.parser.ParserException;

/**
 * 
 * @author CrazyPig
 * @since 2017-02-21
 *
 */
public class MycatConfigCreateHandler {
	
	private static final Logger LOGGER = Logger.getLogger(MycatConfigCreateHandler.class);
	
	public static void handle(String sql, ManagerConnection c) {
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		try {
			SQLStatement stmt = parser.parseStatement();
			if(stmt instanceof MycatCreateSchemaStatement) {
				CreateSchemaHandler.handle(c, (MycatCreateSchemaStatement) stmt, sql);
			} else if(stmt instanceof MycatCreateTableStatement) {
				CreateTableHandler.handle(c, (MycatCreateTableStatement) stmt, sql);
			} else if(stmt instanceof MycatCreateChildTableStatement) {
				CreateChildTableHandler.handle(c, (MycatCreateChildTableStatement) stmt, sql);
			} else if(stmt instanceof MycatCreateDataNodeStatement) {
				CreateDataNodeHandler.handle(c, (MycatCreateDataNodeStatement) stmt, sql);
			} else if(stmt instanceof MycatCreateDataHostStatement) {
				CreateDataHostHandler.handle(c, (MycatCreateDataHostStatement) stmt, sql);
			} else if(stmt instanceof MycatCreateUserStatement) {
				CreateUserHandler.handle(c, (MycatCreateUserStatement) stmt, sql);
			} else if(stmt instanceof MycatCreateRuleStatement) {
				CreateRuleHandler.handle(c, (MycatCreateRuleStatement) stmt, sql);
			} else if(stmt instanceof MycatCreateFunctionStatement) {
				CreateFunctionHandler.handle(c, (MycatCreateFunctionStatement) stmt, sql);
			} else if(stmt instanceof MycatCreateMapFileStatement) {
				CreateMapFileHandler.handle(c, (MycatCreateMapFileStatement) stmt, sql);
			} else {
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "Unsupport statement : " + sql);
			} 
		} catch(ParserException e) {
			c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, e.getMessage());
		} catch(Exception e) {
			LOGGER.error(e.getMessage(), e);
			c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, e.getMessage());
		}
	}
	
}
