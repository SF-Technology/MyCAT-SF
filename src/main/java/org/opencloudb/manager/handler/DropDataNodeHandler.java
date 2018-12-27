package org.opencloudb.manager.handler;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.backend.PhysicalDBPool;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.loader.xml.jaxb.DatabaseJAXB;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.config.util.ConfigTar;
import org.opencloudb.config.util.JAXBUtil;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.statement.MycatDropDataNodeStatement;
import org.opencloudb.net.mysql.OkPacket;

/**
 * @author 01140003
 * @version 2017年4月26日 下午5:58:36
 */
public class DropDataNodeHandler {
	private static final Logger LOGGER = Logger.getLogger(DropDataNodeHandler.class);

	public static void handle(ManagerConnection c, MycatDropDataNodeStatement stmt, String sql) {
		MycatConfig mycatConf = MycatServer.getInstance().getConfig();

		mycatConf.getLock().lock();

		try {
			c.setLastOperation("drop datanode " + stmt.getDataNode().getSimpleName()); // 记录操作
			Map<String, PhysicalDBNode> dataNodes = mycatConf.getDataNodes();
			Map<String, PhysicalDBPool> dataHosts = mycatConf.getDataHosts();

			String nodeName = stmt.getDataNode().getSimpleName();

			if (dataNodes.get(nodeName) == null) {
				c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "DataNode " + nodeName + " dosen't exist.");
				return;
			}

			if (isReferred(nodeName)) {
				c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "DataNode " + nodeName + " is in use.");
				return;
			}

			// 生成dataNode信息的副本，更新副本的信息，然后将其刷入文件中
			Map<String, PhysicalDBNode> dataNodesCopy = new TreeMap<String, PhysicalDBNode>(dataNodes);
			dataNodesCopy.remove(nodeName);
			DatabaseJAXB databaseJAXB = JAXBUtil.toDatabaseJAXB(dataNodesCopy, dataHosts);

			if (!JAXBUtil.flushDatabase(databaseJAXB)) {
				c.writeErrMessage(ErrorCode.ER_FLUSH_FAILED, "flush database.xml failed.");
				return;
			}

			// 更新内存中的dataNode信息
			dataNodes.remove(nodeName);
			
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
			c.setLastOperation("drop datanode " + stmt.getDataNode().getSimpleName()); // 记录操作
			
			LOGGER.error(e.getMessage(), e);
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
		} finally {
			mycatConf.getLock().unlock();
		}
	}

	/**
	 * 检查一个datanode是否已经被schema或着table引用
	 * 
	 * @param nodeName
	 * @return
	 */
	private static boolean isReferred(String nodeName) {
		MycatConfig mycatConf = MycatServer.getInstance().getConfig();
		Collection<SchemaConfig> schemas = mycatConf.getSchemas().values();

		for (SchemaConfig schema : schemas) {
			if (schema.getAllDataNodes().contains(nodeName)) { // 检查schema引用的datanode
				return true;
			}
			Collection<TableConfig> tables = schema.getTables().values();
			for (TableConfig table : tables) {
				if (table.getDataNodes().contains(nodeName)) { // 检查table引用的datanode
					return true;
				}
			}
		}

		return false;
	}
}
