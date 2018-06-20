package org.opencloudb.manager.handler;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.log4j.Logger;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.config.util.ConfigTar;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.statement.MycatDropMapFileStatement;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.route.function.AbstractPartitionAlgorithm;
import org.opencloudb.route.function.AutoPartitionByLong;
import org.opencloudb.route.function.PartitionByFileMap;
import org.opencloudb.route.function.PartitionByPattern;
import org.opencloudb.route.function.PartitionByPrefixPattern;
import org.opencloudb.route.function.PartitionByRangeMod;

public class DropMapFileHandler {
	private static final Logger LOGGER = Logger.getLogger(DropMapFileHandler.class);

	public static void handle(ManagerConnection c, MycatDropMapFileStatement stmt, String sql) {
		String fileName = stmt.getFileName();

		MycatConfig mycatConf = MycatServer.getInstance().getConfig();

		mycatConf.getLock().lock();
		
		try {
			c.setLastOperation("drop mapfile " + stmt.getFileName()); // 记录操作
			
			String mapFilePath = null;
			try {
				mapFilePath = acquireMapFilePath(fileName);
			} catch (IOException e) {
				c.writeErrMessage(ErrorCode.ER_FILE_USED, "fail to acquire mapfile path.");
				LOGGER.error("fail to acquire mapfile path.", e);
				return;
			}

			File mapFile = new File(mapFilePath);

			if (!mapFile.exists()) { // mapfile不存在
				c.writeErrMessage(ErrorCode.ER_FILE_NOT_FOUND, "Mapfile " + fileName + " is not found.");
				return;
			}
			
			if (isReferred(fileName)) { // mapfile被某个function引用
				c.writeErrMessage(ErrorCode.ER_FILE_USED, "Mapfile " + fileName + " is in use.");
				return;
			}
			
			mapFile.delete();

			// 对配置信息进行备份
			try {
				ConfigTar.tarConfig(c.getLastOperation());
			} catch (Exception e) {
				throw new Exception("Fail to do backup.");
			}
			
			// 响应客户端
			ByteBuffer buffer = c.allocate();
			c.write(c.writeToBuffer(OkPacket.OK, buffer));
			return;
		} catch (Exception e) {
			c.setLastOperation("drop mapfile " + stmt.getFileName()); // 记录操作
			
			LOGGER.error(e.getMessage(), e);
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
		} finally {
			mycatConf.getLock().unlock();
		}
		
	}

	/**
	 * 获得获得mapfile的路径
	 * 
	 * @param fileName
	 *            MapFile文件
	 * @return
	 * @throws IOException
	 */
	private static String acquireMapFilePath(String fileName) throws IOException {
		String classPath = SystemConfig.class.getClassLoader().getResource("").getPath();
		classPath = deleteLastSlash(classPath);

		return classPath + File.separatorChar + SystemConfig.getMapFileFolder() + File.separatorChar + fileName;
	}

	/**
	 * 取出路径末尾的多余字符，包括斜杠，反斜杠和空白字符
	 * 
	 * @param str
	 * @return
	 */
	private static String deleteLastSlash(String str) {
		str = str.trim();

		switch (str.charAt(str.length() - 1)) {
		case '/':
		case '\\':
			str = str.substring(0, str.length() - 1);
			break;

		default:
			break;
		}

		return str;
	}

	private static boolean isReferred(String fileName) {
		MycatConfig mycatConf = MycatServer.getInstance().getConfig();
		Map<String, AbstractPartitionAlgorithm> functions = mycatConf.getFunctions();

		for (AbstractPartitionAlgorithm func : functions.values()) {
			if (func instanceof AutoPartitionByLong) {
				if (((AutoPartitionByLong) func).getMapFile().equals(fileName)) {
					return true;
				}
			}

			if (func instanceof PartitionByFileMap) {
				if (((PartitionByFileMap) func).getMapFile().equals(fileName)) {
					return true;
				}
			}

			if (func instanceof PartitionByPattern) {
				if (((PartitionByPattern) func).getMapFile().equals(fileName)) {
					return true;
				}
			}

			if (func instanceof PartitionByPrefixPattern) {
				if (((PartitionByPrefixPattern) func).getMapFile().equals(fileName)) {
					return true;
				}
			}

			if (func instanceof PartitionByRangeMod) {
				if (((PartitionByRangeMod) func).getMapFile().equals(fileName)) {
					return true;
				}
			}

		}

		return false;
	}

}
