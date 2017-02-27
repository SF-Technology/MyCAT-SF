package org.opencloudb.manager.handler;

import org.opencloudb.config.ErrorCode;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.MycatManageStatementParser;
import org.opencloudb.manager.parser.druid.statement.MycatSetSystemVariableStatement;

import com.alibaba.druid.sql.ast.SQLStatement;

public class MycatConfigSetHandler {
public static void handle(String sql, ManagerConnection c) {
		
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		try {
			SQLStatement stmt = parser.parseStatement();
			if(stmt instanceof MycatSetSystemVariableStatement) {
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "need to process set system variable stmt");
			} 
		} catch(Exception e) {
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
		}
	}
}
