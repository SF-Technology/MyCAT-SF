package org.opencloudb.manager.handler;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.loader.xml.jaxb.UserJAXB;
import org.opencloudb.config.model.UserConfig;
import org.opencloudb.config.util.JAXBUtil;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.statement.MycatAlterUserStatement;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.util.SplitUtil;

import com.alibaba.druid.sql.ast.expr.SQLCharExpr;

/**
 * alter user 逻辑处理器
 * @author CrazyPig
 * @since 2017-02-23
 *
 */
public class AlterUserHandler {
	
	
	public static void handle(ManagerConnection c, MycatAlterUserStatement stmt, String sql) {
		
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
			
			UserConfig user = mycatConfig.getUsers().get(userName);
			
			UserConfig newUser = new UserConfig();
			newUser.setName(userName);
			newUser.setPassword(user.getPassword());
			newUser.setEncryptPassword(user.getEncryptPassword());
			newUser.setReadOnly(user.isReadOnly());
			newUser.setSchemas(new HashSet<String>(user.getSchemas()));
			
			if(stmt.isAlterPassword()) {
				String newPassword = ((SQLCharExpr)stmt.getPassword()).getText();
				newUser.setPassword(newPassword);
			}
			
			if(stmt.isAlterSchemas()) {
				String schemas = ((SQLCharExpr)stmt.getSchemas()).getText();
				if(schemas != null) {
	            	String[] strArray = SplitUtil.split(schemas, ',', true);
	            	for(String schema : strArray) {
	            		if(!mycatConfig.getSchemas().containsKey(schema)) {
	            			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "schema '" + schema + "' dosen't exist");
	        				return ;
	            		}
	            	}
	            	newUser.setSchemas(new HashSet<String>(Arrays.asList(strArray)));
	            } else {
	            	newUser.setSchemas(new HashSet<String>());
	            }
			}
			
			if(stmt.isAlterReadOnly()) {
				newUser.setReadOnly(stmt.isReadOnly());
			}
			
            
            // 刷新 user.xml
            Map<String, UserConfig> users = mycatConfig.getUsers();
            users.put(userName, newUser);
            UserJAXB userJAXB = JAXBUtil.toUserJAXB(users, true);
            if(!JAXBUtil.flushUser(userJAXB)) {
            	// flush失败, 需要回滚内存中配置
            	users.put(userName, user);
            	c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "flush user.xml fail");
            	return ;
            }
            
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
