package org.opencloudb.manager.response;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.opencloudb.config.Fields;
import org.opencloudb.config.util.ConfigTar;
import org.opencloudb.config.util.ConfigTar.BackupFile;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.util.StringUtil;

public class ListBackups {
	private static final Logger LOGGER = Logger.getLogger(ListBackups.class);
	
	private static final int FIELD_COUNT = 3;
	private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();

	static {
		int i = 0;
		byte packetId = 1;

		header.packetId = packetId++;

		fields[i] = PacketUtil.getField("index", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = packetId++;

		fields[i] = PacketUtil.getField("opration", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = packetId++;

		fields[i] = PacketUtil.getField("time", Fields.FIELD_TYPE_VAR_STRING);
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
		TreeMap<Long, BackupFile> tarFileMap = ConfigTar.getBackupFileMap().getTarFileMap();
		ArrayList<Long> idList = new ArrayList<Long>(tarFileMap.descendingKeySet());
		int j = 0;
		for (Long timestamp : idList) {
			RowDataPacket row = new RowDataPacket(FIELD_COUNT);

			row.add(StringUtil.encode(Integer.toString(j ++), c.getCharset()));
			row.add(StringUtil.encode(tarFileMap.get(timestamp).getOperation(), c.getCharset()));
			row.add(StringUtil.encode(toTime(timestamp), c.getCharset()));

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
	
	/**
	 * 将时间戳转化为日期
	 * @param timestamp
	 * @return
	 */
	private static String toTime(long timestamp) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(timestamp);
		
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		return format.format(cal.getTime());
	}
}
