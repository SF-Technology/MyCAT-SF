package org.opencloudb.manager.response;

import java.nio.ByteBuffer;
import java.util.Map;

import org.opencloudb.MycatServer;
import org.opencloudb.config.Fields;
import org.opencloudb.config.model.rule.TableRuleConfig;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.util.StringUtil;

/**
 * mycat_config list rules命令响应
 * 
 * @author 01140003
 * @version 2017年2月21日 下午5:27:43
 */
public class ListRules {
	private static final int FIELD_COUNT = 3;
	private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();

	static {
		int i = 0;
		byte packetId = 1;

		header.packetId = packetId++;

		fields[i] = PacketUtil.getField("ruleName", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = packetId++;

		fields[i] = PacketUtil.getField("columns", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = packetId++;

		fields[i] = PacketUtil.getField("function", Fields.FIELD_TYPE_VAR_STRING);
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
		Map<String, TableRuleConfig> tableRules = MycatServer.getInstance().getConfig().getTableRules();
		for (String ruleName : tableRules.keySet()){
			RowDataPacket row = new RowDataPacket(FIELD_COUNT);
			
			row.add(StringUtil.encode(ruleName, c.getCharset()));
			row.add(StringUtil.encode(tableRules.get(ruleName).getRule().getColumn(), c.getCharset()));
			row.add(StringUtil.encode(tableRules.get(ruleName).getRule().getFunctionName(), c.getCharset()));
			
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
