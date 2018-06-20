package org.opencloudb.manager.handler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.Fields;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.statement.MycatShowMapFileStatement;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.util.StringUtil;

public class ShowMapFileHandler {
	private static final Logger LOGGER = Logger.getLogger(CreateSchemaHandler.class);
	
	private static final int FIELD_COUNT = 1;
	private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();

	static {
		int i = 0;
		byte packetId = 1;

		header.packetId = packetId++;

		fields[i] = PacketUtil.getField("contents", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = packetId++;

		eof.packetId = packetId++;
	}


	public static void handle(ManagerConnection c, MycatShowMapFileStatement stmt, String sql) {
		String fileName = stmt.getFileName();
		
		// 获取mapfile
		File mapFile = null;
		try {
			mapFile = new File(acquireMapFilePath(fileName));
			
			if (! mapFile.exists()) {
				c.writeErrMessage(ErrorCode.ER_FILE_EXISTS_ERROR, "mapfile " + fileName + " do not exist.");
				return;
			}
		} catch (IOException e) {
			c.writeErrMessage(ErrorCode.ER_FILE_USED, "fail to acquire mapfile path.");
			LOGGER.error("fail to acquire mapfile path.", e);
			return;
		}
		
		// 逐行获取mapfile中的文本内容
		List<String> lines = null;
		try {
			lines = readLinesFromFile(mapFile);
		} catch (IOException e) {
			c.writeErrMessage(ErrorCode.ER_FILE_USED, "fail to acquire contents from mapfile path.");
			LOGGER.error("fail to acquire mapfile path.", e);
			return;
		}
		
		// 向客户端发送mapfile中的内容
		returnResultSet(c, lines);
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
	
	/**
	 * 从文件逐行读取文本
	 * @param file
	 * @return
	 * @throws IOException 
	 */
	private static List<String> readLinesFromFile (File file) throws IOException {
		List<String> lines = new ArrayList<String>();
		
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(  
			        new FileInputStream(file)));
			
			for (String line = br.readLine(); line != null; line = br.readLine()) {  
	            lines.add(line);  
	        }  
			
		} catch (FileNotFoundException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return lines;
	}
	
	/**
	 * 向客户端输出mapfile中的内容
	 * @param c
	 * @param lines
	 */
	public static void returnResultSet(ManagerConnection c, List<String> lines) {
		ByteBuffer buffer = c.allocate();

		// write header
		buffer = header.write(buffer, c, true);

		// write fields
		for (FieldPacket field : fields) {
			buffer = field.write(buffer, c, true);
		}

		// write eof
		buffer = eof.write(buffer, c, true);

		byte packetId = eof.packetId;

		// write rows
		for (String line : lines) {
			RowDataPacket row = new RowDataPacket(FIELD_COUNT);

			row.add(StringUtil.encode(line, c.getCharset()));
			row.packetId = ++packetId;

			buffer = row.write(buffer, c, true);
		}

		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.write(buffer, c, true);

		// post write
		c.write(buffer);
	}
}
