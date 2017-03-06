package org.opencloudb.manager.handler;

import org.opencloudb.config.ErrorCode;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.MycatManageStatementParser;
import org.opencloudb.manager.parser.druid.statement.MycatSetSystemVariableStatement;

import com.alibaba.druid.sql.ast.SQLStatement;

/**
 * @author 01140003
 * @version 2017年3月2日 下午7:21:31 
 */
public class MycatConfigSetHandler {
public static void handle(String sql, ManagerConnection c) {
		
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		try {
			SQLStatement stmt = parser.parseStatement();
			if(stmt instanceof MycatSetSystemVariableStatement) {
				SetSystemVariableHandler.handle(c, (MycatSetSystemVariableStatement)stmt, sql);
			} 
		} catch(Exception e) {
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
		}
	}
}
