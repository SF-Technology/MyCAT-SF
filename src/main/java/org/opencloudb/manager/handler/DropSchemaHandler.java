package org.opencloudb.manager.handler;

import java.nio.ByteBuffer;
import java.util.Map;

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.UserConfig;
import org.opencloudb.config.util.JAXBUtil;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.statement.MycatDropSchemaStatement;
import org.opencloudb.net.mysql.OkPacket;

/**
 * drop schema 逻辑处理器
 * @author CrazyPig
 * @since 2017-02-16
 *
 */
public class DropSchemaHandler {
	
	public static void handle(ManagerConnection c, MycatDropSchemaStatement stmt, String sql) {
		
		String schemaName = stmt.getSchema().toString();
		MycatConfig mycatConf = MycatServer.getInstance().getConfig();
		mycatConf.getLock().lock();
		
		try {
			
			Map<String, SchemaConfig> schemas = mycatConf.getSchemas();
			// 检查schema是否存在
			if(!schemas.containsKey(schemaName)) {
				c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, 
						"schema '" + schemaName + "' dosen't exist");
				return ;
			}
			
			// 对于引用该schema的用户, 需要删除对应schema集合中该schema
			boolean needFlushRule = false;
			for(UserConfig userConf : mycatConf.getUsers().values()) {
				if(userConf.getSchemas().contains(schemaName)) {
					userConf.getSchemas().remove(schemaName);
					needFlushRule = true;
				}
			} 
			
			schemas.remove(schemaName);
			
			// 刷新 schema.xml
			JAXBUtil.flushSchema(mycatConf);
			
			// 刷新 user.xml
			if(needFlushRule) {
				JAXBUtil.flushRule(mycatConf);
			}
			
			if(schemaName.equals(c.getSchema())) {
				c.setSchema(null);
			}
			
			ByteBuffer buffer = c.allocate();
			c.write(c.writeToBuffer(OkPacket.OK, buffer));
			
		} catch(Exception e) {
			e.printStackTrace();
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
		} finally {
			mycatConf.getLock().unlock();
		}
		
	}

}
