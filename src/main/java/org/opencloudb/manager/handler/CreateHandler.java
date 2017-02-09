package org.opencloudb.manager.handler;

import org.opencloudb.config.ErrorCode;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.MycatManageStatementParser;
import org.opencloudb.manager.parser.druid.statement.MycatCreateChildTableStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateDataHostStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateDataNodeStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateSchemaStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateTableStatement;

import com.alibaba.druid.sql.ast.SQLStatement;

public class CreateHandler {
	
	public static void handle(String sql, ManagerConnection c) {
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		try {
			SQLStatement stmt = parser.parseStatement();
			if(stmt instanceof MycatCreateSchemaStatement) {
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "need to process create schema stmt");
			} else if(stmt instanceof MycatCreateTableStatement) {
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "need to process create table stmt");
			} else if(stmt instanceof MycatCreateChildTableStatement) {
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "need to process create childtable stmt");
			} else if(stmt instanceof MycatCreateDataNodeStatement) {
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "need to process create datanode stmt");
			} else if(stmt instanceof MycatCreateDataHostStatement) {
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "need to process create datahost stmt");
			}
		} catch(Exception e) {
			c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, e.getMessage());
		}
	}

}
