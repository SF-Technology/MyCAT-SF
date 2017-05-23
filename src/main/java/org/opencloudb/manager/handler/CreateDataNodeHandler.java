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
import org.opencloudb.manager.parser.druid.statement.MycatCreateDataNodeStatement;
import org.opencloudb.net.mysql.OkPacket;

import com.alibaba.druid.sql.ast.expr.SQLCharExpr;

/**
 * 处理创建dataNode的命令
 * @author 01140003
 * @version 2017年4月26日 上午10:26:26
 */
public class CreateDataNodeHandler {
	public static final Logger LOGGER = Logger.getLogger(CreateDataNodeHandler.class);

	public static void handle(ManagerConnection c, MycatCreateDataNodeStatement stmt, String sql) {
		MycatConfig mycatConf = MycatServer.getInstance().getConfig();

		mycatConf.getLock().lock();

		try {
			c.setLastOperation("create datanode " + stmt.getDatanode().getSimpleName()); // 记录操作
			
			Map<String, PhysicalDBNode> dataNodes = mycatConf.getDataNodes();
			Map<String, PhysicalDBPool> dataHosts = mycatConf.getDataHosts();
			String nodeName = stmt.getDatanode().getSimpleName();
			String hostName = ((SQLCharExpr) stmt.getDatahost()).getValue().toString();
			String database = ((SQLCharExpr) stmt.getDatabase()).getValue().toString();
			PhysicalDBPool dbPool = dataHosts.get(hostName);

			if (dataNodes.get(nodeName) != null) {
				c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "DataNode " + nodeName + " already exists.");
				return;
			}

			if (dbPool == null) {
				c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "DataHost " + hostName + " dosen't exist.");
				return;
			}

			// 新的dataNode
			PhysicalDBNode dataNode = new PhysicalDBNode(nodeName, database, dbPool);

			// 生成dataNode信息的副本，更新副本的信息，然后将其刷入文件中
			Map<String, PhysicalDBNode> dataNodesCopy = new TreeMap<String, PhysicalDBNode>(dataNodes);
			dataNodesCopy.put(nodeName, dataNode);
			DatabaseJAXB databaseJAXB = JAXBUtil.toDatabaseJAXB(dataNodesCopy, dataHosts);

			if (!JAXBUtil.flushDatabase(databaseJAXB)) {
				c.writeErrMessage(ErrorCode.ER_FLUSH_FAILED, "flush database.xml failed.");
				return;
			}

			// 更新内存中的dataNode信息
			dataNodes.put(nodeName, dataNode);
			
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
			c.setLastOperation("create datanode " + stmt.getDatanode().getSimpleName()); // 记录操作
			
			LOGGER.error(e.getMessage(), e);
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
		} finally {
			mycatConf.getLock().unlock();
		}
	}
}
