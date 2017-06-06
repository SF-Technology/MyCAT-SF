package org.opencloudb.manager.response;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.Fields;
import org.opencloudb.config.model.ProcedureConfig;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.util.StringUtil;

import com.google.common.base.Strings;

/**
 * 
 * @author 01169238
 * @since 1.5.3
 *
 */
public class ListProcedures {
    
    private static final int FIELD_COUNT = 2;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        fields[i] = PacketUtil.getField("name", Fields.FIELD_TYPE_VAR_STRING);
        fields[i].packetId = ++packetId;
        
        i++;
        
        fields[i] = PacketUtil.getField("dataNode", Fields.FIELD_TYPE_VAR_STRING);
        fields[i].packetId = ++packetId;
        
        eof.packetId = ++packetId;
    }

    public static void response(ManagerConnection c) {
        String schema = c.getSchema();
        if (Strings.isNullOrEmpty(schema)) {
            c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
            return;
        }
        MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
        SchemaConfig schemaConfig = mycatConfig.getSchemas().get(schema);
        Map<String, ProcedureConfig> procedures = schemaConfig.getProcedures();

        Set<String> procedureNameOrderedSet = new TreeSet<>(procedures.keySet());

        ByteBuffer buffer = c.allocate();
        // write header
        buffer = header.write(buffer, c, true);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, c, true);
        }

        // write eof
        buffer = eof.write(buffer, c, true);

        // write rows
        byte packetId = eof.packetId;

        for (String procedureName : procedureNameOrderedSet) {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(StringUtil.encode(procedureName.toLowerCase(), c.getCharset()));
            row.add(StringUtil.encode(procedures.get(procedureName).getDataNode(), c.getCharset()));
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
