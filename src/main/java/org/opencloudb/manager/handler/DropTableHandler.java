package org.opencloudb.manager.handler;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.loader.xml.jaxb.SchemaJAXB;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.config.util.JAXBUtil;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.statement.MycatDropTableStatement;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.util.StringUtil;

/**
 * drop table 逻辑处理器 (包含drop childtable)
 * @author CrazyPig
 * @since 2017-02-16
 *
 */
public class DropTableHandler {
	
	public static void handle(ManagerConnection c, MycatDropTableStatement stmt, String sql) {

		String tableName = StringUtil.removeBackquote(stmt.getTable().getSimpleName());
		String upperTableName = tableName.toUpperCase();
		MycatConfig mycatConf = MycatServer.getInstance().getConfig();
		
		mycatConf.getLock().lock();
		try {
			
			// 获取当前schema
			String schemaName = c.getSchema();
			// (可选)检查schema是否存在
			if(!mycatConf.getSchemas().containsKey(schemaName)) {
				c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "schema '" + schemaName + "' dosen't exist");
				return ;
			}
			
			SchemaConfig schemaConf = mycatConf.getSchemas().get(schemaName);
			/*
			 * schema dataNode属性不为空, 表示该schema不是sharding schema, 不能在此schema上创建table或者drop table
			 */
			if(schemaConf.getDataNode() != null) {
				c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "noSharding schema can not create or drop table");
				return ;
			}
			
			// 检查table是否已经存在
			if(!schemaConf.getTables().containsKey(upperTableName)) {
				c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "table '" + tableName + "' dosen't exist");
				return ;
			}
			
			TableConfig target = schemaConf.getTables().get(upperTableName);
			
			// 检查是否有子表, 如果有子表也要drop掉
			List<TableConfig> delChildTables = new ArrayList<TableConfig>();
			for(Iterator<TableConfig> it = schemaConf.getTables().values().iterator(); it.hasNext();) {
				TableConfig tableConf = it.next();
				if(tableConf == target) {
					continue;
				}
				if(tableConf.isChildTable() && isChild(tableConf, target)) {
					delChildTables.add(tableConf);
					it.remove();
				}
			}
			
			TableConfig delTable = schemaConf.getTables().remove(target.getName());
			
			// 刷新 schema.xml
			SchemaJAXB schemaJAXB = JAXBUtil.toSchemaJAXB(mycatConf.getSchemas());
			if(!JAXBUtil.flushSchema(schemaJAXB)) {
				// 出错回滚
				schemaConf.getTables().put(upperTableName, delTable);
				for(TableConfig delChildTable : delChildTables) {
					schemaConf.getTables().put(delChildTable.getName(), delChildTable);
				}
				c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "flush schema.xml fail");
				return ;
			}
			
			// 重新更新SchemaConfig datanode集合
			schemaConf.updateDataNodesMeta();
			
			ByteBuffer buffer = c.allocate();
			c.write(c.writeToBuffer(OkPacket.OK, buffer));
			
		} catch(Exception e) {
			e.printStackTrace();
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
		} finally {
			mycatConf.getLock().unlock();
		}
		
	}
	
	private static boolean isChild(TableConfig child, TableConfig ancestor) {
		TableConfig parent = null;
		while((parent = child.getParentTC()) != null) {
			if(parent == ancestor) {
				return true;
			}
			child = parent;
		}
		return false;
	}

}
