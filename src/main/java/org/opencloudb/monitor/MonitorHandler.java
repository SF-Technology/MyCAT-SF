package org.opencloudb.monitor;

import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.Fields;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.memory.unsafe.utils.BytesTools;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.sqlfw.H2DBManager;
import org.opencloudb.util.IntegerUtil;
import org.opencloudb.util.LongUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zagnix
 * @create 2016-11-01 13:53
 */

public class MonitorHandler {

    private final static Logger LOGGER =
            LoggerFactory.getLogger(MonitorHandler.class);

    private static  List<ColMetaData>
            listHeader= new ArrayList<ColMetaData>();


    private static  int FIELD_COUNT =0;
    private static  ResultSetHeaderPacket header = null;
    private static  FieldPacket[] fields =null;
    private static  EOFPacket eof = new EOFPacket();

    public static void execute(ManagerConnection c,String sql) {
        query(sql,c);
    }

    public static void query(String sql,ManagerConnection c) {


        Connection dbConn = null;
        Statement stmt = null;
        ResultSet rset = null;

        if (sql !=null && (sql.indexOf(H2DBManager.getSqlBackListTableName()) !=-1 ||
                           sql.indexOf(H2DBManager.getSqlReporterTableName()) !=-1)){
            dbConn = H2DBManager.getH2DBManager().getH2DBConn();
        }else {
            dbConn = H2DBMonitorManager.
                    getH2DBMonitorManager().getH2DBMonitorConn();
        }

        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("sql === >  " + sql);
            }
            stmt = dbConn.createStatement();
            rset = stmt.executeQuery(sql);

            FIELD_COUNT  = rset.getMetaData().getColumnCount();

            /**
             * 初始化 header.....
             */
            byte packetId = 0;
            int n = 0;
            header = PacketUtil.getHeader(FIELD_COUNT);
            fields = new FieldPacket[FIELD_COUNT];
            header.packetId = ++packetId;
            for (int i = 1; i <=FIELD_COUNT; i++) {
                String colname = rset.getMetaData().getColumnName(i).toLowerCase();
                int colType = rset.getMetaData().getColumnType(i);
                switch (colType){
                    case Types.VARCHAR:/**12*/
                        fields[n] = PacketUtil.getField(colname, Fields.FIELD_TYPE_VAR_STRING);
                        fields[n++].packetId = ++packetId;
                        break;
                    case Types.BIGINT:/**-5*/
                        fields[n] = PacketUtil.getField(colname, Fields.FIELD_TYPE_LONGLONG);
                        fields[n++].packetId = ++packetId;
                        break;
                    case Types.INTEGER: /**4*/
                        fields[n] = PacketUtil.getField(colname, Fields.FIELD_TYPE_LONG);
                        fields[n++].packetId = ++packetId;
                        break;
                    case Types.DOUBLE:
                        fields[n] = PacketUtil.getField(colname, Fields.FIELD_TYPE_DOUBLE);
                        fields[n++].packetId = ++packetId;
                        break;
                    default:
                        break;
                }

                if (LOGGER.isDebugEnabled()) {
                    ColMetaData colMetaData = new ColMetaData(colname,colType);
                    LOGGER.error(colMetaData.toString());
                }
            }

            eof.packetId = ++packetId;
            ByteBuffer buffer = c.allocate();
            /**
             *  write header
             **/
            buffer = header.write(buffer, c,true);

            /**
             * write fields
             */
            for (FieldPacket field : fields) {
                buffer = field.write(buffer, c,true);
            }
            /**
             * write eof
             */
            buffer = eof.write(buffer,c,true);

            /**
             *   write rows
             */
            packetId = eof.packetId;
            while (rset.next()) {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                for (int i = 1; i <= FIELD_COUNT; i++) {
                    int colType = rset.getMetaData().getColumnType(i);
                    switch (colType) {
                        case Types.VARCHAR:/**12*/
                            try {
                                row.add(rset.getString(i).getBytes(c.getCharset()));
                            } catch (UnsupportedEncodingException e) {
                                LOGGER.error(e.getMessage());
                            }
                            break;
                        case Types.BIGINT:/**-5*/
                            row.add(LongUtil.toBytes(rset.getLong(i)));
                            break;

                        case Types.INTEGER: /**4*/
                            row.add(IntegerUtil.toBytes(rset.getInt(i)));
                            break;
                        case Types.DOUBLE:/**8*/
                            try {
                                row.add(BytesTools.double2Bytes(rset.getDouble(i)));
                            } catch (UnsupportedEncodingException e) {
                                e.printStackTrace();
                            };
                            break;
                        default:
                            break;
                    }
                }
                row.packetId = ++packetId;
                buffer = row.write(buffer, c, true);
            }
            /**
             * write last eof
              */
            EOFPacket lastEof = new EOFPacket();
            lastEof.packetId = ++packetId;
            buffer = lastEof.write(buffer, c,true);

            /**
             * post write
             */
            c.write(buffer);

        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
            c.writeErrMessage(ErrorCode.ER_YES, e.getMessage());
        }finally {
            try {
                if(stmt !=null){
                    stmt.close();
                }
                if (rset !=null){
                    rset.close();
                }
            } catch (SQLException e) {
                LOGGER.error(e.getMessage());
                c.writeErrMessage(ErrorCode.ER_YES, e.getMessage());
            }
        }
        return;
    }
}
