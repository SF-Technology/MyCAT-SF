package org.opencloudb.manager.handler;

import org.apache.log4j.Logger;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.MycatManageStatementParser;
import org.opencloudb.manager.parser.druid.statement.MycatAlterUserStatement;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.parser.ParserException;

/**
 * 
 * @author CrazyPig
 * @since 2017-02-23
 *
 */
public class MycatConfigAlterHandler {
	
	private static final Logger LOGGER = Logger.getLogger(MycatConfigAlterHandler.class);
	
	public static void handle(String sql, ManagerConnection c) {
		
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		try {
			SQLStatement stmt = parser.parseStatement();
			if(stmt instanceof MycatAlterUserStatement) {
				AlterUserHandler.handle(c, (MycatAlterUserStatement) stmt, sql);
			} else {
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
