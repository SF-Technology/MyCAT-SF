package org.opencloudb.manager.handler;

import org.opencloudb.config.ErrorCode;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.MycatManageStatementParser;
import org.opencloudb.manager.parser.druid.statement.MycatCreateChildTableStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateDataHostStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateDataNodeStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateSchemaStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateTableStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateUserStatement;

import com.alibaba.druid.sql.ast.SQLStatement;

/**
 * 
 * @author CrazyPig
 * @since 2017-02-21
 *
 */
public class MycatConfigCreateHandler {
	
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
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "Unsupport create datanode stmt");
			} else if(stmt instanceof MycatCreateDataHostStatement) {
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "Unsupport create datahost stmt");
			} else if(stmt instanceof MycatCreateUserStatement) {
				CreateUserHandler.handle(c, (MycatCreateUserStatement) stmt, sql);
			} else {
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "Unsupport statement : " + sql);
			}
		} catch(Exception e) {
			e.printStackTrace();
			c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, e.getMessage());
		}
	}

}
