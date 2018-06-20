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
import org.opencloudb.config.util.ConfigTar;
import org.opencloudb.config.util.JAXBUtil;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.statement.MycatDropFunctionStatement;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.route.function.AbstractPartitionAlgorithm;

/**
 * 处理drop function的逻辑
 * 
 * @author 01140003
 * @version 2017年3月1日 下午5:50:10
 */
public class DropFunctionHandler {
	private static final Logger LOGGER = Logger.getLogger(DropFunctionHandler.class);

	public static void handle(ManagerConnection c, MycatDropFunctionStatement stmt, String sql) {
		MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
		mycatConfig.getLock().lock();

		try {
			c.setLastOperation("drop function " + stmt.getName().getSimpleName()); // 记录操作
			
			Map<String, TableRuleConfig> tableRules = mycatConfig.getTableRules();
			Map<String, AbstractPartitionAlgorithm> functions = mycatConfig.getFunctions();

			String funcName = stmt.getName().getSimpleName();

			if (functions.get(funcName) == null) { // not exist
				c.writeErrMessage(ErrorCode.ER_FUNCTION_NOT_EXIST, "Function named " + funcName + " dosen't exist.");
				return;
			} else if (functionInUse(funcName, tableRules)) { // function in use
				c.writeErrMessage(ErrorCode.ER_FUNCTION_CANT_REMOVE, "Function named " + funcName + " is in use.");
				return;
			} else {
				Map<String, AbstractPartitionAlgorithm> tempFunctions = new HashMap<String, AbstractPartitionAlgorithm>(
						functions);
				tempFunctions.remove(funcName);

				// 将修改刷到rule.xml中
				RuleJAXB ruleJAXB = JAXBUtil.toRuleJAXB(tableRules, tempFunctions);
				if (!JAXBUtil.flushRule(ruleJAXB)) {
					c.writeErrMessage(ErrorCode.ER_FLUSH_FAILED, "flush rule.xml fail");
					return;
				}

				// rule.xml刷成功之后，更新内存中的配置信息
				functions.remove(funcName);

				// 对配置信息进行备份
				try {
					ConfigTar.tarConfig(c.getLastOperation());
				} catch (Exception e) {
					throw new Exception("Fail to do backup.");
				}
				
				// 向客户端发送ok包
				ByteBuffer buffer = c.allocate();
				c.write(c.writeToBuffer(OkPacket.OK, buffer));
			}
		} catch (Exception e) {
			c.setLastOperation("drop function " + stmt.getName().getSimpleName()); // 记录操作
			
			LOGGER.error(e.getMessage(), e);
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
		} finally {
			mycatConfig.getLock().unlock();
		}

	}

	/**
	 * 判断分片算法是否被某个分片规则使用
	 * 
	 * @param funcName
	 * @param tableRules
	 * @return
	 */
	private static boolean functionInUse(String funcName, Map<String, TableRuleConfig> tableRules) {
		for (TableRuleConfig ruleConfig : tableRules.values()) {
			if (ruleConfig.getRule().getFunctionName().equals(funcName)) {
				return true;
			}
		}

		return false;
	}
}
