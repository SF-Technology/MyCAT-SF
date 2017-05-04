package org.opencloudb.manager.handler;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.Fields;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.statement.MycatListVariablesStatement;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.util.StringUtil;

public class ListSystemVariablesHandler {
	private static final Logger LOGGER = Logger.getLogger(ListSystemVariablesHandler.class);
	
	private static final int FIELD_COUNT = 4;
	private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	
	static {
		int i = 0;
		byte packetId = 1;

		header.packetId = packetId++;

		fields[i] = PacketUtil.getField("varName", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = packetId++;

		fields[i] = PacketUtil.getField("curValue", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = packetId++;

		fields[i] = PacketUtil.getField("defValue", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = packetId++;
		
		fields[i] = PacketUtil.getField("dynamic", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = packetId++;

		eof.packetId = packetId++;
	}
	
	public static void handle(ManagerConnection c, MycatListVariablesStatement stmt, String sql) {
		String matchExpr = stmt.getMatchExpr();
		
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
		Map<String, Object> currentSytemVariables;
		Map<String, Object> defaultSystemVariables;
		Set<String> dynamicVariables;
		SystemConfig systemConfig = MycatServer.getInstance().getConfig().getSystem();
		try {
			currentSytemVariables = systemConfig.currentSystemVariables();
			defaultSystemVariables = systemConfig.defaultSystemVariables();
			dynamicVariables = systemConfig.dynamicVariables();
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| IntrospectionException | SecurityException e) {
			c.writeErrMessage(ErrorCode.ER_CANT_GET_SYSTEM_VARIABLES, "fail to get system variables.");
			LOGGER.error(e.getMessage(), e);
			return;
		}
		
		for (String varName : currentSytemVariables.keySet()){
			
			Object curValue;
			Object defValue;
			
			if (varName.equals("rootPassword")) {
				curValue = "*";
				defValue = "*";
			} else {
				curValue = currentSytemVariables.get(varName);
				defValue = defaultSystemVariables.get(varName);
			}
			
			
			if (curValue == null) {
				curValue = "null";
			}
			
			if (defValue == null) {
				defValue = "null";
			}
			
			if (match(varName, matchExpr)) {
				RowDataPacket row = new RowDataPacket(FIELD_COUNT);
				
				row.add(StringUtil.encode(varName, c.getCharset()));
				row.add(StringUtil.encode(curValue.toString(), c.getCharset()));
				row.add(StringUtil.encode(defValue.toString(), c.getCharset()));
				row.add(StringUtil.encode(String.valueOf(dynamicVariables.contains(varName)), c.getCharset()));
				
				row.packetId = ++packetId;
				
				buffer = row.write(buffer, c, true);
			}
		}
		
		// write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c, true);

        // post write
        c.write(buffer);
	}
	
	/**
	 * 检查目标字符串varName是否匹配%name%表达式
	 * @param varName
	 * @param matchExpr
	 * @return
	 */
	private static boolean match(String varName, String matchExpr) {
		if (matchExpr == null) { // 没有匹配表达式
			return true;
		}
		
		if (matchExpr.equals("")) {
			return false;
		}
		
		int len = matchExpr.length();
		if(matchExpr.charAt(0) == '%') {
			if (matchExpr.charAt(len - 1) == '%') {
				if (len == 1) {
					return false;
				}
				return varName.contains(matchExpr.substring(1, len - 1));
			} else {
				return varName.endsWith(matchExpr.substring(1, len));
			}
		} else {
			if (matchExpr.charAt(len - 1) == '%') {
				return varName.startsWith(matchExpr.substring(0, len - 1));
			} else {
				return varName.equals(matchExpr);
			}
		}
	}
}
