package org.opencloudb.manager.handler;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.log4j.Logger;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.loader.xml.jaxb.UserJAXB;
import org.opencloudb.config.model.UserConfig;
import org.opencloudb.config.util.ConfigTar;
import org.opencloudb.config.util.JAXBUtil;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.statement.MycatDropUserStatement;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.util.StringUtil;


/**
 * drop user 逻辑处理器
 * @author CrazyPig
 * @since 2017-02-23
 *
 */
public class DropUserHandler {
	
	private static final Logger LOGGER = Logger.getLogger(DropUserHandler.class);
	
	public static void handle(ManagerConnection c, MycatDropUserStatement stmt, String sql) {
		
		if(!DynamicConfigPrivilegesHandler.isPrivilegesOk(c)) {
			c.writeErrMessage(ErrorCode.ER_ACCESS_DENIED_ERROR, "This command can only be used with build-in root user");
			return ;
		}
		
		MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
		mycatConfig.getLock().lock();
		
		try {
			c.setLastOperation("drop user " + stmt.getUserName().getSimpleName()); // 记录操作
			
			String userName = StringUtil.removeBackquote(stmt.getUserName().getSimpleName());
			// 判断user是否已经存在
			if(!mycatConfig.getUsers().containsKey(userName)) {
				c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "user '" + userName + "' dosen't exist");
				return ;
			}
			
			// 不能删除root用户
			if(mycatConfig.getSystem().getRootUser().equals(userName)) {
				c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "user '" + userName + "' is the built-in root user, can not be drop");
				return ;
			}
			
            // 刷新 user.xml
			Map<String, UserConfig> users = mycatConfig.getUsers();
			UserConfig delUser = users.remove(userName);
			UserJAXB userJAXB = JAXBUtil.toUserJAXB(users, true);
            if(!JAXBUtil.flushUser(userJAXB)) {
            	users.put(userName, delUser); // flush失败, 回滚内存中配置
            	c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "flush user.xml fail");
            	return ;
            }
            
			// 对配置信息进行备份
			try {
				ConfigTar.tarConfig(c.getLastOperation());
			} catch (Exception e) {
				throw new Exception("Fail to do backup.");
			}
            
			// 响应客户端
            ByteBuffer buffer = c.allocate();
			c.write(c.writeToBuffer(OkPacket.OK, buffer));
			
		} catch(Exception e) {
			c.setLastOperation("drop user " + stmt.getUserName().getSimpleName()); // 记录操作
			
			LOGGER.error(e.getMessage(), e);
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
		} finally {
			mycatConfig.getLock().unlock();
		}
		
	}
	
}
