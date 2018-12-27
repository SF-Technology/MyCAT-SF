package org.opencloudb.manager.handler;

import org.apache.log4j.Logger;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.util.ConfigTar;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.MycatManageStatementParser;
import org.opencloudb.manager.parser.druid.statement.MycatConfigRollbackStatement;
import org.opencloudb.response.ReloadConfig;

import com.alibaba.druid.sql.ast.SQLStatement;

public class MycatConfigRollbackHandler {
	private static final Logger LOGGER = Logger.getLogger(MycatConfigRollbackHandler.class);
	
	public static void handle(String sql, ManagerConnection c) {

		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		try {
			SQLStatement stmt = parser.parseStatement();
			if (stmt instanceof MycatConfigRollbackStatement) {
				handle(c, (MycatConfigRollbackStatement) stmt, sql);
			} else {
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "Unsupport statement : " + sql);
			}
		} catch (Exception e) {
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
			LOGGER.error(e.getMessage(), e);
		}
	}
	
	/**
	 * 处理mycat_config rollback命令的处理
	 * @param c
	 * @param stmt
	 * @param sql
	 */
	public static void handle(ManagerConnection c, MycatConfigRollbackStatement stmt, String sql) {
		MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
		mycatConfig.getLock().lock();
		
		try {
			int index = stmt.getIndex();
			
			if (! ConfigTar.getBackupFileMap().containsIndex(index)) {
				c.writeErrMessage(ErrorCode.ER_FILE_NOT_FOUND, "Rollback index " + index + " is out of boundary.");
				return;
			}
			
			ConfigTar.untarConfig(index);
			
			ReloadConfig.rollbackReload(c, index);
			return;
		} catch(Exception e) {
			LOGGER.error(e.getMessage(), e);
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
		} finally {
			mycatConfig.getLock().unlock();
		}
	}
}
