package org.opencloudb.response;

import org.h2.util.StringUtils;
import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.Fields;
import org.opencloudb.config.model.QuarantineConfig;
import org.opencloudb.config.model.UserConfig;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.memory.unsafe.utils.BytesTools;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.*;
import org.opencloudb.sqlfw.SQLFirewallServer;
import org.opencloudb.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public final class ShowSQLBackList {
	private static final Logger LOGGER = LoggerFactory.getLogger(ShowSQLBackList.class);

    private static final int FIELD_COUNT = 2;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        fields[i] = PacketUtil.getField("id", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("sql", Fields.FIELD_TYPE_VARCHAR);
        fields[i++].packetId = ++packetId;
        eof.packetId = ++packetId;
    }
    
	public static void execute(ManagerConnection c) {
        ByteBuffer buffer = c.allocate();

        // write header
        buffer = header.write(buffer, c,true);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, c,true);
        }

        // write eof
        buffer = eof.write(buffer, c,true);

        // write rows
        byte packetId = eof.packetId;

        ConcurrentHashMap<Integer, String> sqlBlackListMap =
                MycatServer.getInstance().getSqlFirewallServer().getSqlBackListMap();

        for (int key:sqlBlackListMap.keySet()) {
            RowDataPacket row = getRow(key,sqlBlackListMap.get(key),c.getCharset());
            row.packetId = ++packetId;
            buffer = row.write(buffer,c,true);
        }
		
        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c,true);

        // write buffer
        c.write(buffer);		
	}
    private static RowDataPacket getRow(int sqlId,String sql,String charset) {
    	RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        try {
            row.add(BytesTools.int2Bytes(sqlId));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        row.add( StringUtil.encode( sql, charset) );
        return row;
    }

    public static String parseString(String stmt) {
   	 int offset = stmt.indexOf(',');
        if (offset != -1 && stmt.length() > ++offset) {
            String txt = stmt.substring(offset).trim();
            return txt;
        }
        return null;
   }


	public static void addSqlBackList(ManagerConnection c,String sql) {

	    OkPacket ok = new OkPacket();

        LOGGER.error("add sql : " + sql);

       SQLFirewallServer sqlFirewallServer =
                MycatServer.getInstance().getSqlFirewallServer();

       if (sqlFirewallServer.addSqlToBlacklist(sql)){
           ok.packetId = 1;
           ok.affectedRows = 1;
           ok.serverStatus = 2;        
    	   ok.message = "add sql to blacklist succeed.".getBytes();
           ok.write(c);
       } else {
           c.writeErrMessage(ErrorCode.ER_YES, "sql add to blacklist falied.");
       }
	}

    public static void removeSqlFromBackList(ManagerConnection c,String id) {

        OkPacket ok = new OkPacket();

        if(id == null ){
            c.writeErrMessage(ErrorCode.ER_YES, " sql id  is null.");
            return;
        }
        int sql_id = 0;

        if (id != null && StringUtils.isNumber(id)){
             sql_id = Integer.valueOf(id);
         }else {
            c.writeErrMessage(ErrorCode.ER_YES, "sql id must be number .");
            return;
        }

        SQLFirewallServer sqlFirewallServer =
                MycatServer.getInstance().getSqlFirewallServer();

        if (sqlFirewallServer.removeSqlfromBackList(sql_id)){
            ok.packetId = 1;
            ok.affectedRows = 1;
            ok.serverStatus = 2;
            ok.message = "remove sql from blacklist succeed.".getBytes();
            ok.write(c);
        } else {
            c.writeErrMessage(ErrorCode.ER_YES, "remove sql from blacklist falied.");
        }
    }



}
