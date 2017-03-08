package org.opencloudb.manager.handler;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.loader.xml.jaxb.RuleJAXB;
import org.opencloudb.config.model.rule.TableRuleConfig;
import org.opencloudb.config.util.JAXBUtil;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.statement.MycatDropRuleStatement;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.route.function.AbstractPartitionAlgorithm;

/**
 * 处理drop rule语句的逻辑
 * @author 01140003
 * @version 2017年3月1日 下午5:18:04 
 */
public class DropRuleHandler {
	private static final Logger LOGGER = Logger.getLogger(DropRuleHandler.class);
	public static void handle(ManagerConnection c, MycatDropRuleStatement stmt, String sql) {
		MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
		Map<String, TableRuleConfig> tableRules = mycatConfig.getTableRules();
		Map<String, AbstractPartitionAlgorithm> functions = mycatConfig.getFunctions();
		
		String ruleName = stmt.getRule();
		
		if (tableRules.get(ruleName) == null) {
			c.writeErrMessage(ErrorCode.ER_RULE_NOT_EXIST, "Rule named " + ruleName + " dosen't exist.");
			return;
		} else {
			// 拷贝tableRules
			Map<String, TableRuleConfig> tempTableRules = new HashMap<String, TableRuleConfig>(tableRules);
			tempTableRules.remove(ruleName);
			
			// 将修改刷到rule.xml中
			try {
				RuleJAXB ruleJAXB = JAXBUtil.toRuleJAXB(tempTableRules, functions);
				if(!JAXBUtil.flushRule(ruleJAXB)) {
					c.writeErrMessage(ErrorCode.ER_FLUSH_FAILED, "flush rule.xml fail");
					return ;
				}
			} catch (Exception e) {
				c.writeErrMessage(ErrorCode.ER_FLUSH_FAILED, "flush rule.xml fail");
				LOGGER.error("flush rule.xml fail",e);
				return ;
			}
			
			tableRules.remove(ruleName);
			
			// 向客户端端发送ok包
			ByteBuffer buffer = c.allocate();
			c.write(c.writeToBuffer(OkPacket.OK, buffer));
		}
	}
}
