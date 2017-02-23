package org.opencloudb.manager.handler;

import org.opencloudb.config.ErrorCode;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.MycatManageStatementParser;
import org.opencloudb.manager.parser.druid.statement.MycatCreateUserStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropDataHostStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropDataNodeStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropSchemaStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropTableStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropUserStatement;

import com.alibaba.druid.sql.ast.SQLStatement;

/**
 * 
 * @author CrazyPig
 * @since 2017-02-21
 *
 */
public class MycatConfigDropHandler {
	
	public static void handle(String sql, ManagerConnection c) {
		
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		try {
			SQLStatement stmt = parser.parseStatement();
			if(stmt instanceof MycatDropSchemaStatement) {
				DropSchemaHandler.handle(c, (MycatDropSchemaStatement) stmt, sql);
			} else if(stmt instanceof MycatDropTableStatement) {
				DropTableHandler.handle(c, (MycatDropTableStatement) stmt, sql);
			} else if(stmt instanceof MycatDropDataNodeStatement) {
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "need to process drop datanode stmt");
			} else if(stmt instanceof MycatDropDataHostStatement) {
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "need to process drop datahost stmt");
			} else if(stmt instanceof MycatDropUserStatement) {
				DropUserHandler.handle(c, (MycatDropUserStatement) stmt, sql);
			} else { // TODO more... 
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "Unsupport statement : " + sql);
			}
		} catch(Exception e) {
			e.printStackTrace();
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
		}
	}

}
