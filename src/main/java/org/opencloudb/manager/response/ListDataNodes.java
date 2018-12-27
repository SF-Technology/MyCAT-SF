package org.opencloudb.manager.response;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeSet;

import org.opencloudb.MycatServer;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.config.Fields;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.util.StringUtil;

/**
 * list datanodes 语句响应
 * @author 01169238
 *
 */
public class ListDataNodes {
	
	private static final int FIELD_COUNT = 3;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    
    static {
    	int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        fields[i] = PacketUtil.getField("dnName", Fields.FIELD_TYPE_VAR_STRING);
        fields[i].packetId = ++packetId;
        
        i++;
        
        fields[i] = PacketUtil.getField("dataHost", Fields.FIELD_TYPE_VAR_STRING);
        fields[i].packetId = ++packetId;
        
        i++;
        
        fields[i] = PacketUtil.getField("database", Fields.FIELD_TYPE_VAR_STRING);
        fields[i].packetId = ++packetId;
        
        eof.packetId = ++packetId;
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
		Map<String, PhysicalDBNode> dataNodes = MycatServer.getInstance().getConfig().getDataNodes();
		for(String dnName : new TreeSet<>(dataNodes.keySet())) {
			PhysicalDBNode dataNode = dataNodes.get(dnName);
			RowDataPacket row = new RowDataPacket(FIELD_COUNT);
			row.add(StringUtil.encode(dnName, c.getCharset()));
			row.add(StringUtil.encode(dataNode.getDbPool().getHostName(), c.getCharset()));
			row.add(StringUtil.encode(dataNode.getDatabase(), c.getCharset()));
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
