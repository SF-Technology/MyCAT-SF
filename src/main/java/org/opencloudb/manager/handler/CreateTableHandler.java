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
import org.opencloudb.config.model.rule.RuleConfig;
import org.opencloudb.config.model.rule.TableRuleConfig;
import org.opencloudb.config.util.JAXBUtil;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.statement.MycatCreateTableStatement;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.util.SplitUtil;
import org.opencloudb.util.StringUtil;

import com.alibaba.druid.sql.ast.expr.SQLCharExpr;

/**
 * create table 逻辑处理器
 * @author CrazyPig
 * @since 2017-02-22
 *
 */
public class CreateTableHandler {
	
	private static final Logger LOGGER = Logger.getLogger(CreateTableHandler.class);
	
	private static final Set<String> DEFAULT_DB_TYPE_SET = new HashSet<String>();
	
	static {
		DEFAULT_DB_TYPE_SET.add("mysql");
	}
	
	public static void handle(ManagerConnection c, MycatCreateTableStatement stmt, String sql) {
		
		MycatConfig mycatConf = MycatServer.getInstance().getConfig();
		mycatConf.getLock().lock();
		try {
			
			String schemaName = (stmt.getSchema() == null ? c.getSchema() : StringUtil.removeBackquote(stmt.getSchema().getSimpleName()));
			
			if(!mycatConf.getUsers().get(c.getUser()).getSchemas().contains(schemaName)) {
				c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + schemaName + "'");
				return ;
			}
			
			SchemaConfig schemaConf = mycatConf.getSchemas().get(schemaName);
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
			boolean needAddLimit = stmt.isNeedAddLimit();
			int tableType = stmt.isGlobal() ? TableConfig.TYPE_GLOBAL_TABLE : TableConfig.TYPE_GLOBAL_DEFAULT;
			String dataNode = ((SQLCharExpr)stmt.getDataNodes()).getText();
			String ruleName = stmt.getRule() == null ? null : ((SQLCharExpr)stmt.getRule()).getText();
			
			// 验证表是否存在
			if(schemaConf.getTables().containsKey(upperTableName)) {
				c.writeErrMessage(ErrorCode.ER_TABLE_EXISTS_ERROR, "table '" + tableName + "' already exist");
				return ;
			}
			// 验证dataNode是否存在
			String[] dataNodes = SplitUtil.split(dataNode, ',', '$', '-');
			for(String _dataNode : dataNodes) {
				if(!mycatConf.getDataNodes().containsKey(_dataNode)) {
					c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "dataNode '" + _dataNode + "' dosen't exist");
					return ;
				}
			}
			RuleConfig rule = null;
			if(ruleName != null) {
				// 验证rule是否存在
				TableRuleConfig tableRuleConf = mycatConf.getTableRules().get(ruleName);
				if(tableRuleConf == null) {
					c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "rule '" + ruleName + "' dosen't exist");
					return ;
				}
				rule = tableRuleConf.getRule();
			}
			boolean ruleRequired = false;
			TableConfig tableConf = new TableConfig(upperTableName, primaryKey, autoIncrement, 
					needAddLimit, tableType, dataNode, 
					DEFAULT_DB_TYPE_SET, rule, ruleRequired, 
					null, false, null, null);
			
			schemaConf.getTables().put(upperTableName, tableConf);

			SchemaJAXB schemaJAXB = JAXBUtil.toSchemaJAXB(mycatConf.getSchemas());
			
			// 刷新 schema.xml
			if(!JAXBUtil.flushSchema(schemaJAXB)) {
				// 出错回滚
				schemaConf.getTables().remove(upperTableName);
				c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "flush schema.xml fail");
				return ;
			}
			
			// 更新datanode, Tips: 引入的table有可能增加新的datanode
			schemaConf.updateDataNodesMeta();
			
			
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
