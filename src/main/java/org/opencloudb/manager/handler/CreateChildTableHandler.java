package org.opencloudb.manager.handler;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.loader.xml.jaxb.SchemaJAXB;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.config.model.UserConfig;
import org.opencloudb.config.util.JAXBUtil;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.statement.MycatCreateChildTableStatement;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.util.StringUtil;

import com.alibaba.druid.sql.ast.expr.SQLCharExpr;

/**
 * create childtable 逻辑处理器
 * @author CrazyPig
 * @since 2017-02-22
 *
 */
public class CreateChildTableHandler {
	
	private static final Logger LOGGER = Logger.getLogger(CreateChildTableHandler.class);
	
	private static final Set<String> DEFAULT_DB_TYPE_SET = new HashSet<String>();
	
	static {
		DEFAULT_DB_TYPE_SET.add("mysql");
	}
	
	public static void handle(ManagerConnection c, MycatCreateChildTableStatement stmt, String sql) {
		
		MycatConfig mycatConf = MycatServer.getInstance().getConfig();
		mycatConf.getLock().lock();
		try {
			
			String schemaName = (stmt.getSchema() == null ? c.getSchema() : StringUtil.removeBackquote(stmt.getSchema().getSimpleName()));
			
			UserConfig userConf = mycatConf.getUsers().get(c.getUser());
            if (userConf == null) {
                c.writeErrMessage(ErrorCode.ER_NO_SUCH_USER, "Unknown user '" + c.getUser() + "'");
                return ;
            }
			
			if(!userConf.getSchemas().contains(schemaName)) {
				c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + schemaName + "'");
				return ;
			}
			
			SchemaConfig schemaConf = mycatConf.getSchemas().get(schemaName);
			
			// 验证schema是否存在
			if(schemaConf == null) {
				c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + schemaName + "'");
				return ;
			}
			
			/*
			 * schema dataNode属性不为空, 表示该schema不是sharding schema, 不能在此schema上创建table或者drop table
			 */
			if(schemaConf.getDataNode() != null) {
				c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "noSharding schema can not create or drop table");
				return ;
			}
		
			String tableName = StringUtil.removeBackquote(stmt.getTable().getSimpleName());
			String upperTableName = tableName.toUpperCase();
			String primaryKey = stmt.getPrimaryKey() == null ? null : ((SQLCharExpr)stmt.getPrimaryKey()).getText();
			boolean autoIncrement = false;
			boolean needAddLimit = true;
			int tableType = TableConfig.TYPE_GLOBAL_DEFAULT;
			String joinKey = ((SQLCharExpr)stmt.getJoinKey()).getText();
			String parentKey = ((SQLCharExpr)stmt.getParentKey()).getText();
			
			// 验证表是否存在
			if(schemaConf.getTables().containsKey(upperTableName)) {
				c.writeErrMessage(ErrorCode.ER_TABLE_EXISTS_ERROR, "table '" + tableName + "' already exist");
				return ;
			}
			
			String parentTableName = ((SQLCharExpr)stmt.getParentTable()).getText();
			// 验证parent table是否存在
			TableConfig parentTC = schemaConf.getTables().get(parentTableName.toUpperCase());
			if(parentTC == null) {
				c.writeErrMessage(ErrorCode.ER_NO_SUCH_TABLE, "parent table '" + parentTableName + "' dosen't exist");
				return ;
			}
			
			// 验证parent table必须是顶级分片表 或者 子表
			if(parentTC.getParentTC() == null) { // 父表是顶级表
				if(parentTC.getRule() == null || parentTC.isGlobalTable()) { // 没有分片规则
					c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "parent table '" + parentTableName + "' is not a shard table");
					return ;
				}
			}
			
			TableConfig tableConf = new TableConfig(upperTableName, primaryKey, autoIncrement, 
					needAddLimit, tableType, parentTC.getDataNode(), 
					DEFAULT_DB_TYPE_SET, null, false, 
					parentTC, true, joinKey, parentKey);
			
			schemaConf.getTables().put(upperTableName, tableConf);
			
			SchemaJAXB schemaJAXB = JAXBUtil.toSchemaJAXB(mycatConf.getSchemas());
			
			// 刷新 schema.xml
			if(!JAXBUtil.flushSchema(schemaJAXB)) {
				// 出错回滚
				schemaConf.getTables().remove(upperTableName);
				c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "flush schema.xml fail");
				return ;
			}
			
			ByteBuffer buffer = c.allocate();
			c.write(c.writeToBuffer(OkPacket.OK, buffer));
				
		} catch(Exception e) {
			LOGGER.error(e.getMessage(), e);
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
		} finally {
			mycatConf.getLock().unlock();
		}
		
	}
	
}
