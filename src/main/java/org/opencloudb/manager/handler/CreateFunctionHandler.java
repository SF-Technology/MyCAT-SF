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
import org.opencloudb.config.util.ParameterMapping;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.statement.MycatCreateFunctionStatement;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.route.function.AbstractPartitionAlgorithm;

/**
 * 处理create function的逻辑
 * @author 01140003
 * @version 2017年3月1日 下午3:10:43 
 */
public class CreateFunctionHandler {
	private static final Logger LOGGER = Logger.getLogger(CreateFunctionHandler.class);
	public static void handle(ManagerConnection c, MycatCreateFunctionStatement stmt, String sql){
		MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
		Map<String, TableRuleConfig> tableRules = mycatConfig.getTableRules();
		Map<String, AbstractPartitionAlgorithm> functions = mycatConfig.getFunctions();
		
		String name = stmt.getFunction();
		String className = stmt.getClassName();
		Map<String, String> properties = stmt.getProperties();
		
		mycatConfig.getLock().lock();
		
		try {
			if (functions.get(name) != null) { // function exists
				c.writeErrMessage(ErrorCode.ER_CANT_CREATE_FUNCTION, "Function named " + name + " already exists.");
				return;
			}
			
			AbstractPartitionAlgorithm function = createFunction(className);
			
			
			ParameterMapping.mapping(function, properties);
				
			function.init(); // 实例化AbstractPartitionAlgorithm对象后要对它进行初始化
			
			Map<String, AbstractPartitionAlgorithm> tempFunctions = new HashMap<String, AbstractPartitionAlgorithm>(functions);
			tempFunctions.put(name, function);
			
			// 将修改刷到rule.xml中
			RuleJAXB ruleJAXB = JAXBUtil.toRuleJAXB(tableRules, tempFunctions);
			if(!JAXBUtil.flushRule(ruleJAXB)) {
				c.writeErrMessage(ErrorCode.ER_FLUSH_FAILED, "flush rule.xml fail");
				return ;
			}
			
			// rule.xml刷成功之后，更新内存中的配置信息
			functions.put(name, function);
		} catch (ReflectiveOperationException e) {
			c.writeErrMessage(ErrorCode.ER_CANT_CREATE_FUNCTION, "can not mapping the parameters to the " + name + " object.");
			LOGGER.error("can not mapping the parameters to the " + name + " object.", e);
			return;
		} catch (IllegalArgumentException e) {
			c.writeErrMessage(ErrorCode.ER_CANT_CREATE_FUNCTION, "fail to instantiate class " + className);
			LOGGER.error("fail to instantiate class " + className,e);
			return;
		} catch (Exception e) {
			c.writeErrMessage(ErrorCode.ER_FLUSH_FAILED, "flush rule.xml fail");
			LOGGER.error("flush rule.xml fail",e);
			return;
		} finally {
			mycatConfig.getLock().unlock();
		}
		
		// 向客户端发送ok包
		ByteBuffer buffer = c.allocate();
		c.write(c.writeToBuffer(OkPacket.OK, buffer));
	}
	
	/**
	 * 根据类名实例化AbstractPartitionAlgorithm子类的对象
	 * @param className
	 * @return
	 */
	private static AbstractPartitionAlgorithm createFunction(String className) {
		try {
			Class<?> clz = Class.forName(className);
			if (!AbstractPartitionAlgorithm.class.isAssignableFrom(clz)) { // className所对应的类没有继承AbstractPartitionAlgorithm
				throw new IllegalArgumentException("rule function must implements "
						+ AbstractPartitionAlgorithm.class.getName() + ", name=" + className);
			}
			return (AbstractPartitionAlgorithm) clz.newInstance();
		} catch (ClassNotFoundException e) { // class not found
			throw new IllegalArgumentException("no class named " + className + " was found.");
		} catch (ReflectiveOperationException e) { // instantiation failed
			throw new IllegalArgumentException("fail to instantiate class " + className);
		}
	}
}
