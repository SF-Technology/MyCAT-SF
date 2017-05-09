package org.opencloudb.manager.handler;

import org.apache.log4j.Logger;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.MycatManageStatementParser;
import org.opencloudb.manager.parser.druid.statement.MycatShowMapFileStatement;

import com.alibaba.druid.sql.ast.SQLStatement;

public class MycatConfigShowHandler {
	private static final Logger LOGGER = Logger.getLogger(MycatConfigSetHandler.class);
	public static void handle(String sql, ManagerConnection c) {
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		try {
			SQLStatement stmt = parser.parseStatement();
			if (stmt instanceof MycatShowMapFileStatement) {
				ShowMapFileHandler.handle(c, (MycatShowMapFileStatement) stmt, sql);
			}
		} catch (Exception e) {
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
			LOGGER.error(e.getMessage(), e);
		}
	}
}
