package org.opencloudb.response;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.Map;

import org.opencloudb.config.Fields;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.stat.UserRWStat;
import org.opencloudb.stat.UserStat;
import org.opencloudb.stat.UserStatAnalyzer;
import org.opencloudb.util.LongUtil;
import org.opencloudb.util.StringUtil;

/**
 * 查询用户的 SQL 执行情况
 * 
 * 1、用户 R/W数、读占比、并发数
 * 2、请求时间范围
 * 3、请求的耗时范围
 * 
 * @author zhuam
 */
public class ShowSQLSumUser {
	
	private static DecimalFormat decimalFormat = new DecimalFormat("0.00");

    private static final int FIELD_COUNT = 9;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("USER", Fields.FIELD_TYPE_VARCHAR);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("R", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("W", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("R%", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("MAX", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        //22-06h, 06-13h, 13-18h, 18-22h
        fields[i] = PacketUtil.getField("TIME_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;        
        
        //<10ms, 10ms-200ms, 200ms-1s, >1s
        fields[i] = PacketUtil.getField("TTL_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("LAST_TIME", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;
        eof.packetId = ++packetId;
    }

    public static void execute(ManagerConnection c, boolean isClear) {
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
        int i=0;  
        
        Map<String, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
        for (UserStat userStat : statMap.values()) {
        	i++;
           RowDataPacket row = getRow(userStat,i, c.getCharset());//getRow(sqlStat,sql, c.getCharset());
           row.packetId = ++packetId;
           buffer = row.write(buffer, c,true);
           if ( isClear ) {
        	   userStat.clearRwStat(); 
           }
        }
        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c,true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(UserStat userStat, long idx, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(idx));
        if (userStat == null){
        	row.add(StringUtil.encode(("not fond"), charset));
        	return row;
        }
        
        String user = userStat.getUser();
        UserRWStat rwStat = userStat.getRWStat();
        long R = rwStat.getRCount();
        long W = rwStat.getWCount();
        String __R = decimalFormat.format( 1.0D * R / (R + W) );
        int MAX = rwStat.getConcurrentMax();
        
        row.add( StringUtil.encode( user, charset) );
        row.add( LongUtil.toBytes( R ) );
        row.add( LongUtil.toBytes( W ) );
        row.add( StringUtil.encode( String.valueOf( __R ), charset) );
        row.add( StringUtil.encode( String.valueOf( MAX ), charset) );
        row.add( StringUtil.encode( rwStat.getExecuteHistogram().toString(), charset) );
        row.add( StringUtil.encode( rwStat.getTimeHistogram().toString(), charset) );
        row.add( LongUtil.toBytes( rwStat.getLastExecuteTime() ) );
        
        return row;
    }

}
