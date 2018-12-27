package org.opencloudb.manager.handler;

import org.opencloudb.MycatServer;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.manager.ManagerConnection;

public class DynamicConfigPrivilegesHandler {
	
	public static boolean isPrivilegesOk(ManagerConnection c) {
		// 只允许built-in的root用户
		SystemConfig system = MycatServer.getInstance().getConfig().getSystem();
		if(system.getRootUser().equals(c.getUser())) {
			return true;
		}
		return false;
	}
	
	
}
