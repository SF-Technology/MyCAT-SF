package org.opencloudb.manager.response;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.Fields;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.util.StringUtil;

public class ListSystemVariables {
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
	
	public static void response(ManagerConnection c) {
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
			return;
		}
		
		for (String varName : currentSytemVariables.keySet()){
			
			
			Object curValue = currentSytemVariables.get(varName);
			Object defValue = defaultSystemVariables.get(varName);
			
			if (curValue == null) {
				curValue = "null";
			}
			
			if (defValue == null) {
				defValue = "null";
			}
			
			RowDataPacket row = new RowDataPacket(FIELD_COUNT);
			
			row.add(StringUtil.encode(varName, c.getCharset()));
			row.add(StringUtil.encode(curValue.toString(), c.getCharset()));
			row.add(StringUtil.encode(defValue.toString(), c.getCharset()));
			row.add(StringUtil.encode(String.valueOf(dynamicVariables.contains(varName)), c.getCharset()));
			
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
