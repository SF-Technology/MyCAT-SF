package org.opencloudb.manager.handler;

import org.opencloudb.config.ErrorCode;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.MycatManageStatementParser;
import org.opencloudb.manager.parser.druid.statement.MycatDropDataHostStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropDataNodeStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropFunctionStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropRuleStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropSchemaStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropTableStatement;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropFunctionStatement;

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
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "need to process drop schema stmt");
			} else if(stmt instanceof MycatDropTableStatement) {
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "need to process drop table stmt");
			} else if(stmt instanceof MycatDropDataNodeStatement) {
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "need to process drop datanode stmt");
			} else if(stmt instanceof MycatDropDataHostStatement) {
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "need to process drop datahost stmt");
			} else if(stmt instanceof MycatDropRuleStatement){
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "need to process drop rule stmt");
			} else if(stmt instanceof MycatDropFunctionStatement){
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "need to process drop function stmt");
			} else { // TODO more... 
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "Unsupport statement : " + sql);
			}
		} catch(Exception e) {
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
		}
	}

}
