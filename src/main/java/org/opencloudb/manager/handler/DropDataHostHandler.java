package org.opencloudb.manager.handler;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.backend.PhysicalDBPool;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.loader.xml.jaxb.DatabaseJAXB;
import org.opencloudb.config.util.ConfigTar;
import org.opencloudb.config.util.JAXBUtil;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.statement.MycatDropDataHostStatement;
import org.opencloudb.net.mysql.OkPacket;

class DropDataHostHandler {
	private static final Logger LOGGER = Logger.getLogger(DropDataHostHandler.class);
	
	public static void handle(ManagerConnection c, MycatDropDataHostStatement stmt, String sql) {
		MycatConfig mycatConf = MycatServer.getInstance().getConfig();

		mycatConf.getLock().lock();

		try {
			c.setLastOperation("drop datahost " + stmt.getDataHost().getSimpleName()); // 记录操作
			
			Map<String, PhysicalDBNode> dataNodes = mycatConf.getDataNodes();
			Map<String, PhysicalDBPool> dataHosts = mycatConf.getDataHosts();
			
			String hostName = stmt.getDataHost().getSimpleName();
			
			if (isReferred(hostName, dataNodes)) {
				c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "DataHost " + hostName + " is in use.");
				return;
			}
			
			if (! dataHosts.containsKey(hostName)) {
				c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "DataHost " + hostName + " dosen't exist.");
				return;
			}
			
			// 生成dataHost信息的副本，更新副本的信息，然后将其刷入文件中
			Map<String, PhysicalDBPool> dataHostsCopy = new TreeMap<String, PhysicalDBPool>(dataHosts);
			dataHostsCopy.remove(hostName);
			DatabaseJAXB databaseJAXB = JAXBUtil.toDatabaseJAXB(dataNodes, dataHostsCopy);
			
			if (!JAXBUtil.flushDatabase(databaseJAXB)) {
				c.writeErrMessage(ErrorCode.ER_FLUSH_FAILED, "flush database.xml failed.");
				return;
			}
			
			MycatServer.getInstance().removeDataHostIndex(hostName);
			
			dataHosts.get(hostName).clearDataSources("drop datahost");
			
			// 更新内存中的dataNode信息
			dataHosts.remove(hostName);
			
			// 对配置信息进行备份
			try {
				ConfigTar.tarConfig(c.getLastOperation());
			} catch (Exception e) {
				throw new Exception("Fail to do backup.");
			}
			
			// 响应客户端
			ByteBuffer buffer = c.allocate();
			c.write(c.writeToBuffer(OkPacket.OK, buffer));
		} catch (Exception e) {
			c.setLastOperation("drop datahost " + stmt.getDataHost().getSimpleName()); // 记录操作
			
			LOGGER.error(e.getMessage(), e);
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
		} finally {
			mycatConf.getLock().unlock();
		}
	}
	
	/**
	 * 判断一个dataHost是否被dataNode引用
	 * @param hostName
	 * @param dataNodes
	 * @return
	 */
	private static boolean isReferred(String hostName, Map<String, PhysicalDBNode> dataNodes) {
		for (PhysicalDBNode node : dataNodes.values()) {
			PhysicalDBPool pool = node.getDbPool();
			if (pool.getHostName().equals(hostName)) {
				return true;
			}
		}
		
		return false;
	}
}
