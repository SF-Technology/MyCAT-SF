package org.opencloudb.manager.handler;

import java.nio.ByteBuffer;
import java.util.HashMap;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.config.util.JAXBUtil;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.statement.MycatCreateSchemaStatement;
import org.opencloudb.net.mysql.OkPacket;

/**
 * create schema 逻辑处理器
 * @author CrazyPig
 * @since 2017-02-21
 *
 */
public class CreateSchemaHandler {
	
	public static void handle(ManagerConnection c, MycatCreateSchemaStatement stmt, String sql) {
		
		if(!DynamicConfigPrivilegesHandler.isPrivilegesOk(c)) {
			c.writeErrMessage(ErrorCode.ER_ACCESS_DENIED_ERROR, "This command can only be used with build-in root user");
			return ;
		}
		
		String schemaName = stmt.getSchema().getSimpleName();
		int sqlMaxLimit = stmt.getSqlMaxLimit();
		boolean checkSQLschema = stmt.isCheckSQLSchema();
		// 创建新schema配置
		SchemaConfig schema = new SchemaConfig(schemaName, sqlMaxLimit, checkSQLschema, new HashMap<String, TableConfig>());
		MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
		mycatConfig.getLock().lock();
		try {
			mycatConfig.getSchemas().put(schemaName, schema);
			mycatConfig.getUsers().get(c.getUser()).getSchemas().add(schemaName);
			// 刷新 schema.xml
			flushSchema();
			// 响应客户端
			ByteBuffer buffer = c.allocate();
			c.write(c.writeToBuffer(OkPacket.OK, buffer));
		} catch(Exception e) {
			e.printStackTrace();
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
		} finally {
			mycatConfig.getLock().unlock();
		}
		
	}
	
	private static void flushSchema() throws Exception {
		
		JAXBUtil.flushSchema(MycatServer.getInstance().getConfig());
		
	}
	
}
