package org.opencloudb.manager.handler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.log4j.Logger;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.statement.MycatCreateMapFileStatement;
import org.opencloudb.net.mysql.OkPacket;


public class CreateMapFileHandler {
	private static final Logger LOGGER = Logger.getLogger(CreateMapFileHandler.class);

	public static void handle(ManagerConnection c, MycatCreateMapFileStatement stmt, String sql) {
		String fileName = stmt.getFileName();
		List<String> lines = stmt.getLines();
		
		File mapFile = null;
		try {
			mapFile = new File(acquireMapFilePath(fileName));
			
			if (mapFile.exists()) {
				c.writeErrMessage(ErrorCode.ER_FILE_EXISTS_ERROR, "mapfile " + fileName + " exists.");
				return;
			}
			
		} catch (IOException e) {
			c.writeErrMessage(ErrorCode.ER_FILE_USED, "fail to acquire mapfile path.");
			LOGGER.error("fail to acquire mapfile path.", e);
			return;
		}
		
		try {
			writeToFile(lines, mapFile);
		} catch (IOException e) {
			c.writeErrMessage(ErrorCode.ER_FILE_USED, "fail to write mapfile.");
			LOGGER.error("fail to acquire mapfile.", e);
			return;
		}
		
		// 向客户端发送ok包
		ByteBuffer buffer = c.allocate();
		c.write(c.writeToBuffer(OkPacket.OK, buffer));
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
	 * 将文本内容按行写入文件中
	 * @param lines
	 * @param file
	 * @throws IOException
	 */
	private static void writeToFile(List<String> lines, File file) throws IOException {
		FileWriter fw = null;
		BufferedWriter bw = null;
		try {
			fw = new FileWriter(file);
			bw = new BufferedWriter(fw);

			for (String line : lines) {
				bw.write(line);
				bw.newLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (bw != null) {
				bw.flush();
				bw.close();
			}
			if (fw != null) {
//				fw.flush();
				fw.close();
			}
		}

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
