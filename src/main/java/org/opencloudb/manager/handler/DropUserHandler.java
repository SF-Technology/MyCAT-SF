package org.opencloudb.manager.handler;

import java.nio.ByteBuffer;

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.util.JAXBUtil;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.statement.MycatDropUserStatement;
import org.opencloudb.net.mysql.OkPacket;


/**
 * drop user 逻辑处理器
 * @author CrazyPig
 * @since 2017-02-23
 *
 */
public class DropUserHandler {
	
	
	public static void handle(ManagerConnection c, MycatDropUserStatement stmt, String sql) {
		
		if(!DynamicConfigPrivilegesHandler.isPrivilegesOk(c)) {
			c.writeErrMessage(ErrorCode.ER_ACCESS_DENIED_ERROR, "This command can only be used with build-in root user");
			return ;
		}
		
		MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
		mycatConfig.getLock().lock();
		
		try {
			
			String userName = stmt.getUserName().getSimpleName();
			// 判断user是否已经存在
			if(!mycatConfig.getUsers().containsKey(userName)) {
				c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "user '" + userName + "' dosen't exist");
				return ;
			}
			
            mycatConfig.getUsers().remove(userName);
			
            // 刷新 user.xml
            JAXBUtil.flushUser(mycatConfig);
            
            ByteBuffer buffer = c.allocate();
			c.write(c.writeToBuffer(OkPacket.OK, buffer));
			
		} catch(Exception e) {
			e.printStackTrace();
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
		} finally {
			mycatConfig.getLock().unlock();
		}
		
	}
	
}
