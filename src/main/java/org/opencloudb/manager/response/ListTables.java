package org.opencloudb.manager.response;

import java.nio.ByteBuffer;
import java.util.TreeSet;

import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.Fields;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.util.StringUtil;

/**
 * list tables命令响应
 * @author CrazyPig
 * @since 2017-02-09
 *
 */
public class ListTables {
	
	private static final int FIELD_COUNT = 8;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    
    static {
    	int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("tableName", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("parentTable", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("global", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("primaryKey", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("parentKey", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("joinKey", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("rule", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("dataNode", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        eof.packetId = ++packetId;
    }
    
    public static void response(ManagerConnection c) {
    	
    	String schema = c.getSchema();
    	if(schema == null) {
    		c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
    		return;
    	}
    	
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
    	SchemaConfig schemaConf = MycatServer.getInstance().getConfig().getSchemas().get(schema);
    	for(String tableName : new TreeSet<String>(schemaConf.getTables().keySet())) {
    		TableConfig tableConf = schemaConf.getTables().get(tableName);
    		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
    		row.add(StringUtil.encode(tableName, c.getCharset()));
    		row.add(StringUtil.encode(tableConf.getParentTC() == null ? "NULL" : tableConf.getParentTC().getName(), c.getCharset()));
    		row.add(StringUtil.encode(tableConf.getTableType() == TableConfig.TYPE_GLOBAL_TABLE ? "yes" : "no", c.getCharset()));
    		row.add(StringUtil.encode(tableConf.getPrimaryKey(), c.getCharset()));
    		row.add(StringUtil.encode(tableConf.getParentKey(), c.getCharset()));
    		row.add(StringUtil.encode(tableConf.getJoinKey(), c.getCharset()));
    		row.add(StringUtil.encode(tableConf.getRule() == null ? "NULL" : tableConf.getRule().getName(), c.getCharset()));
    		row.add(StringUtil.encode(tableConf.getDataNode(), c.getCharset()));
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
