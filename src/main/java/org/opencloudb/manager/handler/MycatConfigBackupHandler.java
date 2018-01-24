package org.opencloudb.manager.handler;

import org.apache.log4j.Logger;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.util.ConfigTar;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.MycatManageStatementParser;
import org.opencloudb.manager.parser.druid.statement.MycatConfigBackupStatement;
import org.opencloudb.net.mysql.OkPacket;

import com.alibaba.druid.sql.ast.SQLStatement;

public class MycatConfigBackupHandler {
	private static final Logger LOGGER = Logger.getLogger(MycatConfigBackupHandler.class);
	
	public static void handle(String sql, ManagerConnection c) {

		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		try {
			SQLStatement stmt = parser.parseStatement();
			if (stmt instanceof MycatConfigBackupStatement) {
				handle(c);
			} else {
				c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "Unsupport statement : " + sql);
			}
		} catch (Exception e) {
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
			LOGGER.error(e.getMessage(), e);
		}
	}
	
	/**
	 * 处理mycat_config backup命令的处理
	 * @param c
	 */
	public static void handle(ManagerConnection c) {
		MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
		mycatConfig.getLock().lock();
		
		try {
			String operation = c.getLastOperation();
			
			ConfigTar.tarConfig(operation);
			
			OkPacket ok = new OkPacket();
			ok.packetId = 1;
			ok.affectedRows = 0;
			ok.serverStatus = 2;
			ok.message = "Backup success".getBytes();
			ok.write(c);
		} catch(Exception e) {
			LOGGER.error(e.getMessage(), e);
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
		} finally {
			mycatConfig.getLock().unlock();
		}
	}
}
