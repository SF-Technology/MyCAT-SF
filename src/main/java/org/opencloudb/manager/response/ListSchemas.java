package org.opencloudb.manager.response;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.config.Fields;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.util.StringUtil;

/**
 * list schemas 语句响应
 * @author CrazyPig
 * @since 2017-02-09
 *
 */
public class ListSchemas {
	
	private static final int FIELD_COUNT = 3;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    
    static {
    	int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        fields[i] = PacketUtil.getField("schemaName", Fields.FIELD_TYPE_VAR_STRING);
        fields[i].packetId = ++packetId;
        
        i++;
        
        fields[i] = PacketUtil.getField("checkSQLschema", Fields.FIELD_TYPE_VAR_STRING);
        fields[i].packetId = ++packetId;
        
        i++;
        
        fields[i] = PacketUtil.getField("sqlMaxLimit", Fields.FIELD_TYPE_INT24);
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
		MycatConfig mycatConf = MycatServer.getInstance().getConfig();
		Map<String, SchemaConfig> schemas = mycatConf.getSchemas();
		Set<String> userSchemas = mycatConf.getUsers().get(c.getUser()).getSchemas();
		for (String userSchema : new TreeSet<String>(userSchemas)) {
			SchemaConfig schemaConf = schemas.get(userSchema);
			RowDataPacket row = new RowDataPacket(FIELD_COUNT);
			row.add(StringUtil.encode(userSchema, c.getCharset()));
			row.add(StringUtil.encode(String.valueOf(schemaConf.isCheckSQLSchema()), c.getCharset()));
			row.add(StringUtil.encode(String.valueOf(schemaConf.getDefaultMaxLimit()), c.getCharset()));
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
