package org.opencloudb.manager.handler;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.loader.xml.jaxb.RuleJAXB;
import org.opencloudb.config.model.rule.RuleConfig;
import org.opencloudb.config.model.rule.TableRuleConfig;
import org.opencloudb.config.util.ConfigTar;
import org.opencloudb.config.util.JAXBUtil;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.statement.MycatCreateRuleStatement;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.route.function.AbstractPartitionAlgorithm;

/**
 * 处理create rule的逻辑
 * @author 01140003
 * @version 2017年2月28日 下午3:46:53 
 */
public class CreateRuleHandler {
	private static final Logger LOGGER = Logger.getLogger(CreateRuleHandler.class);
	public static void handle(ManagerConnection c, MycatCreateRuleStatement stmt, String sql) {
		MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
		Map<String, TableRuleConfig> tableRules = mycatConfig.getTableRules();
		Map<String, AbstractPartitionAlgorithm> functions = mycatConfig.getFunctions();
		
		String name = stmt.getRule();
		String column = stmt.getColumn();
		String function = stmt.getFunction();
		
		mycatConfig.getLock().lock();
		
		try {
			c.setLastOperation("create rule " + stmt.getRule()); // 记录操作
			// 分片规则已经存在
			if (tableRules.get(name) != null) {
				c.writeErrMessage(ErrorCode.ER_CANT_CREATE_RULE, "Table rule named " + name + " already exists.");
				return;
			}
			
			// 无法找到分片函数
			if (functions.get(function) == null) {
				c.writeErrMessage(ErrorCode.ER_CANT_CREATE_RULE, "Can not find function named " + function);
				return;
			}
			
			RuleConfig ruleConfig = new RuleConfig(column.toUpperCase(), function);
			ruleConfig.setName(name);
			ruleConfig.setRuleAlgorithm(functions.get(function));
			
			// 拷贝tableRules
			Map<String, TableRuleConfig> tempTableRules = new HashMap<String, TableRuleConfig>(tableRules);
			tempTableRules.put(name, new TableRuleConfig(name, ruleConfig));
			
			// 将修改刷到rule.xml中
			RuleJAXB ruleJAXB = JAXBUtil.toRuleJAXB(tempTableRules, functions);
			if(!JAXBUtil.flushRule(ruleJAXB)) {
				c.writeErrMessage(ErrorCode.ER_FLUSH_FAILED, "flush rule.xml fail");
				return ;
			}
			
			// rule.xml刷成功之后，更新内存中的配置信息
			tableRules.put(name, new TableRuleConfig(name, ruleConfig));
			
			// 对配置信息进行备份
			try {
				ConfigTar.tarConfig(c.getLastOperation());
			} catch (Exception e) {
				throw new Exception("Fail to do backup.");
			}
			
			// 向客户端发送ok包
			ByteBuffer buffer = c.allocate();
			c.write(c.writeToBuffer(OkPacket.OK, buffer));
		} catch (Exception e) {
			c.setLastOperation("create rule " + stmt.getRule()); // 记录操作
			
			LOGGER.error(e.getMessage(), e);
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
		} finally {
			mycatConfig.getLock().unlock();
		}
		
	}
}
