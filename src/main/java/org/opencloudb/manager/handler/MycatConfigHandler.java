package org.opencloudb.manager.handler;

import org.opencloudb.config.ErrorCode;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.parser.ManagerParseMycatConfig;

/**
 * 
 * @author CrazyPig
 * @since 2017-02-21
 *
 */
public class MycatConfigHandler {
	
	public static void handle(ManagerConnection c, String stmt, int offset) {
		
		int rs = ManagerParseMycatConfig.parse(stmt, offset);
		switch(rs) {
		case ManagerParseMycatConfig.LIST:
			MycatConfigListHandler.handle(stmt.substring(offset).trim(), c);
			break;
		case ManagerParseMycatConfig.CREATE:
			MycatConfigCreateHandler.handle(stmt.substring(offset).trim(), c);
			break;
		case ManagerParseMycatConfig.DROP:
			MycatConfigDropHandler.handle(stmt.substring(offset).trim(), c);
			break;
		case ManagerParseMycatConfig.DUMP:
            MycatConfigDumpHandler.handle(stmt.substring(offset).trim(), c);
            break;
		case ManagerParseMycatConfig.ALTER:
			MycatConfigAlterHandler.handle(stmt.substring(offset).trim(), c);
			break;
		case ManagerParseMycatConfig.SET:
			MycatConfigSetHandler.handle(stmt.substring(offset).trim(), c);
			break;
		case ManagerParseMycatConfig.SHOW:
			MycatConfigShowHandler.handle(stmt.substring(offset).trim(), c);
			break;
		case ManagerParseMycatConfig.ROLLBACK:
			MycatConfigRollbackHandler.handle(stmt.substring(offset).trim(), c);
			break;
		case ManagerParseMycatConfig.BACKUP:
			MycatConfigBackupHandler.handle(stmt.substring(offset).trim(), c);
			break;
		default:
			c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement : " + stmt);
			break;
		}
	}

}
