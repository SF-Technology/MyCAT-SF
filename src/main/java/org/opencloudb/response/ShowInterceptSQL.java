package org.opencloudb.response;

import org.opencloudb.MycatServer;
import org.opencloudb.config.Fields;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.memory.unsafe.utils.BytesTools;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.sqlfw.H2DBManager;
import org.opencloudb.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * show 拦截的 sql 和 拦截信息
 *
 * @author zagnix
 * @version 1.0
 * @create 2016-10-25 10:03
 */

public class ShowInterceptSQL {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShowInterceptSQL.class);

    private static final int FIELD_COUNT = 3;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {

        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        fields[i] = PacketUtil.getField("sql", Fields.FIELD_TYPE_VARCHAR);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("sql_msg", Fields.FIELD_TYPE_VARCHAR);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("count", Fields.FIELD_TYPE_VARCHAR);
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


        /**
         * 从H2DB中获取 SQL intercept信息
         */
        final Connection h2DBConn = H2DBManager.getH2DBManager().getH2DBConn();
        Statement stmt = null;
        ResultSet rset = null;

        try {

            String sql = "select * from t_sqlreporter";
            stmt = h2DBConn.createStatement();
            rset = stmt.executeQuery(sql);

            while (rset.next()) {
                RowDataPacket row = getRow(rset.getString(1),rset.getString(2),
                        rset.getInt(3),c.getCharset());
                row.packetId = ++packetId;
                buffer = row.write(buffer,c,true);
            }

        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (rset != null) {
                    rset.close();
                }
            } catch (SQLException e) {
                LOGGER.error(e.getMessage());
            }
        }


        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c,true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(String sql,String sqlMsg,int count,String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add( StringUtil.encode( sql, charset) );
        row.add( StringUtil.encode( sqlMsg, charset) );
        try {
            row.add(BytesTools.int2Bytes(count));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return row;
    }
}
