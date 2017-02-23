package org.opencloudb.manager.handler;

import org.opencloudb.config.ErrorCode;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.MycatManageStatementParser;
import org.opencloudb.manager.parser.druid.statement.MycatAlterUserStatement;

import com.alibaba.druid.sql.ast.SQLStatement;

/**
 * 
 * @author CrazyPig
 * @since 2017-02-23
 *
 */
public class MycatConfigAlterHandler {
	
	public static void handle(String sql, ManagerConnection c) {
		
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		try {
			SQLStatement stmt = parser.parseStatement();
			if(stmt instanceof MycatAlterUserStatement) {
				AlterUserHandler.handle(c, (MycatAlterUserStatement) stmt, sql);
			} else {
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "Unsupport statement : " + sql);
			}
		} catch(Exception e) {
			e.printStackTrace();
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
		}
		
	}

}
