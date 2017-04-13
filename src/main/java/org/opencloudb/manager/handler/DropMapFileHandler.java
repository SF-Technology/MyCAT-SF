package org.opencloudb.manager.handler;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.statement.MycatDropMapFileStatement;
import org.opencloudb.net.mysql.OkPacket;

public class DropMapFileHandler {
	private static final Logger LOGGER = Logger.getLogger(DropMapFileHandler.class);
	
	public static void handle(ManagerConnection c, MycatDropMapFileStatement stmt, String sql) {
		String fileName = stmt.getFileName();
		
		String mapFilePath = null;
		try {
			mapFilePath = acquireMapFilePath(fileName);
		} catch (IOException e) {
			c.writeErrMessage(ErrorCode.ER_FILE_USED, "fail to acquire mapfile path.");
			LOGGER.error("fail to acquire mapfile path.", e);
			return;
		}
		
		File mapFile = new File(mapFilePath);
		
		if (!mapFile.exists()) {
			c.writeErrMessage(ErrorCode.ER_FILE_NOT_FOUND, "Mapfile " + fileName + " is not found");
			return;
		} else {
			mapFile.delete();
			
			ByteBuffer buffer = c.allocate();
			c.write(c.writeToBuffer(OkPacket.OK, buffer));
			return;
		}
		
	}
	
	/**
	 * 获得获得mapfile的路径
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
}
