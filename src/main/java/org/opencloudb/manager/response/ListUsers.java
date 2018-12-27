package org.opencloudb.manager.response;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.TreeSet;

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.config.Fields;
import org.opencloudb.config.model.UserConfig;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.util.StringUtil;

import com.google.common.base.Joiner;

/**
 * list users 命令响应
 * @author CrazyPig
 * @since 2017-02-22
 *
 */
public class ListUsers {
	
	private static final int FIELD_COUNT = 4;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    
    static {
    	int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("name", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("schema", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("password", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("readOnly", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
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
		Set<String> userNameSet = new TreeSet<String>(mycatConf.getUsers().keySet());
		String curUser = c.getUser();
		boolean canShowPassword = mycatConf.getSystem().getRootUser().equals(curUser);
		for(String userName : userNameSet) {
			UserConfig userConf = mycatConf.getUsers().get(userName);
			RowDataPacket row = new RowDataPacket(FIELD_COUNT);
			row.add(StringUtil.encode(userName, c.getCharset()));
			String schemas = "";
			if(userConf.getSchemas().size() > 0) {
				schemas = Joiner.on(",").join(new TreeSet<String>(userConf.getSchemas()));
			}
			row.add(StringUtil.encode(schemas, c.getCharset()));
			row.add(StringUtil.encode(canShowPassword ? userConf.getPassword() : "*", c.getCharset()));
			row.add(StringUtil.encode(userConf.isReadOnly() ? "Y" : "N", c.getCharset()));
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
