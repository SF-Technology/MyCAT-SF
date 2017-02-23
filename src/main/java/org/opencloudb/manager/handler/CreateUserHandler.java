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
import org.opencloudb.manager.parser.druid.statement.MycatCreateUserStatement;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.util.SplitUtil;

import com.alibaba.druid.sql.ast.expr.SQLCharExpr;

/**
 * create user 逻辑处理器
 * @author CrazyPig
 * @since 2017-02-22
 *
 */
public class CreateUserHandler {
	
	
	public static void handle(ManagerConnection c, MycatCreateUserStatement stmt, String sql) {
		
		if(!DynamicConfigPrivilegesHandler.isPrivilegesOk(c)) {
			c.writeErrMessage(ErrorCode.ER_ACCESS_DENIED_ERROR, "This command can only be used with build-in root user");
			return ;
		}
		
		MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
		mycatConfig.getLock().lock();
		
		try {
			
			String newUserName = stmt.getUserName().getSimpleName();
			// 判断user是否已经存在
			if(mycatConfig.getUsers().containsKey(newUserName)) {
				c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "user '" + newUserName + "' already exist");
				return ;
			}
			
			String password = ((SQLCharExpr)stmt.getPassword()).getText();
			String schemas = ((SQLCharExpr)stmt.getSchemas()).getText();
			
			UserConfig newUserConf = new UserConfig();
			newUserConf.setName(newUserName);
			newUserConf.setPassword(password);
			
			
            if(schemas != null) {
            	String[] strArray = SplitUtil.split(schemas, ',', true);
            	newUserConf.setSchemas(new HashSet<String>(Arrays.asList(strArray)));
            } else {
            	newUserConf.setSchemas(new HashSet<String>());
            }
            
            // 刷新 user.xml
            Map<String, UserConfig> users = mycatConfig.getUsers();
            // 更新内存中配置
            users.put(newUserName, newUserConf);
            UserJAXB userJAXB = JAXBUtil.toUserJAXB(users, true);
            if(!JAXBUtil.flushUser(userJAXB)) {
            	users.remove(newUserName); // flush失败, 需要回滚内存中配置
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
