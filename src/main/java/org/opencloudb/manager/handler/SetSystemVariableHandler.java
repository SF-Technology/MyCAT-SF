package org.opencloudb.manager.handler;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Set;

import org.apache.log4j.Logger;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.config.util.ParameterMapping;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.statement.MycatSetSystemVariableStatement;
import org.opencloudb.net.mysql.OkPacket;

/**
 * 处理set system variables的逻辑
 * 
 * @author 01140003
 * @version 2017年3月2日 下午7:23:03
 */
public class SetSystemVariableHandler {
	private static final Logger LOGGER = Logger.getLogger(SetSystemVariableHandler.class);

	public static void handle(ManagerConnection c, MycatSetSystemVariableStatement stmt, String sql) {
		MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
		mycatConfig.getLock().lock();

		try {
			SystemConfig systemConfig = mycatConfig.getSystem();
			Set<String> dynamicVariables = systemConfig.dynamicVariables();

			String name = stmt.getVariableName().getSimpleName();
			String value = stmt.getVariableValue();

			HashMap<String, String> params = new HashMap<String, String>();
			params.put(name, value);

			if (!systemConfig.currentSystemVariables().keySet().contains(name)) {
				c.writeErrMessage(ErrorCode.ER_CANT_GET_SYSTEM_VARIABLES,
						name + " dosen't exist in the system variables.");
				return;
			}

			if (dynamicVariables.contains(name)) { // 判断系统变量是否是动态的
				ParameterMapping.mapping(systemConfig, params);

			} else {
				c.writeErrMessage(ErrorCode.ER_CANT_SET_SYSTEM_VARIABLE,
						"Can not set system variable which is not dynamic.");
				return;
			}
		} catch (IllegalAccessException | InvocationTargetException | IllegalArgumentException | IntrospectionException e) {
			c.writeErrMessage(ErrorCode.ER_CANT_SET_SYSTEM_VARIABLE, "Can not set system variable.");
			LOGGER.error(e.getMessage(), e);
			return;
		} finally {
			mycatConfig.getLock().unlock();
		}

		ByteBuffer buffer = c.allocate();
		c.write(c.writeToBuffer(OkPacket.OK, buffer));
	}
}
