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
import org.opencloudb.config.model.FirewallConfig;
import org.opencloudb.config.util.ParameterMapping;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.statement.MycatSetSqlwallVariableStatement;
import org.opencloudb.net.mysql.OkPacket;

public class SetSqlwallVariableHandler {
	private static final Logger LOGGER = Logger.getLogger(SetSqlwallVariableHandler.class);

	public static void handle(ManagerConnection c, MycatSetSqlwallVariableStatement stmt, String sql) {
		MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
		mycatConfig.getLock().lock();

		try {
			FirewallConfig firewallConfig = mycatConfig.getFirewall();
			Set<String> dynamicVariables = firewallConfig.dynamicVariables();

			String name = stmt.getVariableName().getSimpleName();
			String value = stmt.getVariableValue();

			HashMap<String, String> params = new HashMap<String, String>();
			params.put(name, value);

			if (!firewallConfig.currentSqlwallVariables().keySet().contains(name)) {
				c.writeErrMessage(ErrorCode.ER_CANT_GET_SQLWALL_VARIABLES,
						name + " dosen't exist in the sqlwall variables.");
				return;
			}

			if (dynamicVariables.contains(name)) { // 判断系统变量是否是动态的
				try {
					ParameterMapping.mapping(firewallConfig, params);
				} catch (IllegalAccessException | InvocationTargetException e) {
					c.writeErrMessage(ErrorCode.ER_CANT_SET_SQLWALL_VARIABLE, "Can not set sqlwall variable " + name);
					LOGGER.error("Can not set sqlwall variable " + name, e);
					return;
				}
			} else {
				c.writeErrMessage(ErrorCode.ER_CANT_SET_SQLWALL_VARIABLE,
						"Can not set sqlwall variable which is not dynamic.");
				return;
			}
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| IntrospectionException e1) {
			c.writeErrMessage(ErrorCode.ER_VARIABLE_NOT_EXISTS, "Can not get system variables.");
			LOGGER.error("Can not get system variables.", e1);
			return;
		} finally {
			mycatConfig.getLock().unlock();
		}

		ByteBuffer buffer = c.allocate();
		c.write(c.writeToBuffer(OkPacket.OK, buffer));
	}

}
