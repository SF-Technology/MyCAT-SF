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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zagnix
 * @create 2016-11-01 13:53
 */

public class MonitorHandler {

    private final static Logger LOGGER =
            LoggerFactory.getLogger(MonitorHandler.class);

    private static List<ColMetaData>
            listHeader = new ArrayList<ColMetaData>();

    private static final Map<String, String> table_infos = new LinkedHashMap<String, String>();

    static {
        table_infos.put("t_sqlrecord", "记录所有sql执行情况,方便定位异常sql,其数据超过sqlRecordInDiskPeriod后台自动删除");
        table_infos.put("sql_blacklist", "SQL黑名单，存在此表中的sql，如果开启了拦截功能,将会被mycat拦截掉");
        table_infos.put("sql_reporter", "记录sql在mycat被拦截的信息，其中包括sql防火墙开启记录拦截信息选项和sql语句没有包含分片字段二种情况");
        table_infos.put("t_topnrows", "记录当top N 条sql执行的结果集");
        table_infos.put("t_dmemory", "MyCAT堆外内存使用情况,包括网络packet Handler 和 sql结果集汇聚二部分");
        table_infos.put("t_sysparam", "MyCAT系统关键运行变量信息");
        table_infos.put("t_connectpool", "记录后端数据库连接池详细信息");
        table_infos.put("t_tsc", "MyCat 分片节点各表结构一致性检验结果");
        table_infos.put("t_topncount", "记录当top N 条sql执行次数");
        table_infos.put("t_datasource", "记录每个分片datanode信息");
        table_infos.put("t_topntime", "记录当top N 条sql执行时间");
        table_infos.put("t_threadpool", "记录MyCat业务线程池和定时线程信息");
        table_infos.put("t_heartbeat", "记录后端mysql主机存活信息");
        table_infos.put("t_database", "记录MyCat的schema集合");
        table_infos.put("t_sqlsummary", "某类型sql执行情况，如执行次数，执行时间，执行结果集");
        table_infos.put("t_processor", "MyCat Processor相关信息");
        table_infos.put("t_datanode", "记录MyCat datanode分片信息");
        table_infos.put("t_memory", "记录MyCat 堆内内存使用情况");
        table_infos.put("t_dmemory_detail", "查看MyCAT每线程堆外内存详细情况,包括网络packet Handler 和 sql结果集汇聚二部分");
        table_infos.put("t_cache", "记录mycat 缓存信息，主要有ER关系缓存，SQL路由缓存，TableID2DataNodeCache 缓存");
        table_infos.put("t_sqlstat", "记录当前一段时间(通过参数sqlInMemDBPeriod 配置) sql执行情况，方便多维度进行sql统计");
        table_infos.put("t_connection_cli", "记录client连接到mycat的连接信息");
    }

    private static int FIELD_COUNT = 0;
    private static ResultSetHeaderPacket header = null;
    private static FieldPacket[] fields = null;
    private static EOFPacket eof = new EOFPacket();

    public static void execute(ManagerConnection c, String sql) {

        if (sql != null
                && (sql.indexOf("info_tables") != -1
                || sql.indexOf("INFORMATION_SCHEMA") != -1)) {
            /**
             * 从H2DB中查询用户创建的表
             */
            sql = "SELECT table_name FROM INFORMATION_SCHEMA.TABLES where table_type='TABLE'";
            showtables(sql, c);
            return;
        }

        query(sql, c);
    }

    /**
     * 从H2DB中查询用户创建的表
     */
    public static void showtables(String sql, ManagerConnection c) {
        Connection dbConn = null;
        Statement stmt = null;
        ResultSet rset = null;

        /**
         * db sql_fw 中的表
         */
        dbConn = H2DBManager.getH2DBManager().getH2DBConn();

        try {
            stmt = dbConn.createStatement();
            rset = stmt.executeQuery(sql);

            FIELD_COUNT = rset.getMetaData().getColumnCount();

            /**
             * 初始化 header.....
             */
            byte packetId = 0;
            int n = 0;
            header = PacketUtil.getHeader(FIELD_COUNT + 2);
            fields = new FieldPacket[FIELD_COUNT + 2];
            header.packetId = ++packetId;

            for (int i = 1; i <= FIELD_COUNT; i++) {
                String colname = rset.getMetaData().getColumnName(i).toLowerCase();
                int colType = rset.getMetaData().getColumnType(i);
                switch (colType) {
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
            }

            fields[n] = PacketUtil.getField("type", Fields.FIELD_TYPE_VAR_STRING);
            fields[n++].packetId = ++packetId;

            fields[n] = PacketUtil.getField("desc", Fields.FIELD_TYPE_VAR_STRING);
            fields[n++].packetId = ++packetId;

            eof.packetId = ++packetId;
            ByteBuffer buffer = c.allocate();
            /**
             *write header
             **/
            buffer = header.write(buffer, c, true);
            /**
             * write fields
             */
            for (FieldPacket field : fields) {
                buffer = field.write(buffer, c, true);
            }
            /**
             * write eof
             */
            buffer = eof.write(buffer, c, true);

            /**
             *   write rows
             */
            packetId = eof.packetId;
            while (rset.next()) {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT + 2);
                for (int i = 1; i <= FIELD_COUNT; i++) {
                    int colType = rset.getMetaData().getColumnType(i);
                    switch (colType) {
                        case Types.VARCHAR:/**12*/
                            try {
                                String colValue = rset.getString(i).toLowerCase();
                                row.add(colValue.getBytes(c.getCharset()));
                                row.add("disk".getBytes(c.getCharset()));

                                String desc = null;
                                if ((desc = table_infos.get(colValue))!=null)
                                     row.add(desc.getBytes(c.getCharset()));
                                else
                                    row.add("NULL".getBytes(c.getCharset()));

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
                            }
                            ;
                            break;
                        default:
                            break;
                    }
                }

                row.packetId = ++packetId;

                buffer = row.write(buffer, c, true);
            }


            /**
             * db_moniter 中的表
             */
            dbConn = H2DBMonitorManager.
                    getH2DBMonitorManager().getH2DBMonitorConn();
            stmt = dbConn.createStatement();
            rset = stmt.executeQuery(sql);

            while (rset.next()) {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT + 2);
                for (int i = 1; i <= FIELD_COUNT; i++) {
                    int colType = rset.getMetaData().getColumnType(i);
                    switch (colType) {
                        case Types.VARCHAR:/**12*/
                            try {
                                String colValue = rset.getString(i).toLowerCase();
                                row.add(colValue.getBytes(c.getCharset()));
                                row.add("memory".getBytes(c.getCharset()));

                                String desc = null;
                                if ((desc = table_infos.get(colValue))!=null)
                                    row.add(desc.getBytes(c.getCharset()));
                                else
                                    row.add("NULL".getBytes(c.getCharset()));

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
                            }
                            ;
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
            buffer = lastEof.write(buffer, c, true);

            /**
             * post write
             */
            c.write(buffer);

        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
            c.writeErrMessage(ErrorCode.ER_YES, e.getMessage());
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
                c.writeErrMessage(ErrorCode.ER_YES, e.getMessage());
            }
        }
        return;
    }

    public static void query(String sql, ManagerConnection c) {
        Connection dbConn = null;
        Statement stmt = null;
        ResultSet rset = null;

        if (sql != null && (sql.indexOf(H2DBManager.getSqlBackListTableName()) != -1 ||
                sql.indexOf(H2DBManager.getSqlReporterTableName()) != -1 ||
                sql.indexOf(H2DBManager.getSqlRecordTableName()) != -1)) {
            dbConn = H2DBManager.getH2DBManager().getH2DBConn();
        } else {
            dbConn = H2DBMonitorManager.
                    getH2DBMonitorManager().getH2DBMonitorConn();
        }

        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("sql === >  " + sql);
            }
            stmt = dbConn.createStatement();
            rset = stmt.executeQuery(sql);

            FIELD_COUNT = rset.getMetaData().getColumnCount();

            /**
             * 初始化 header.....
             */
            byte packetId = 0;
            int n = 0;
            header = PacketUtil.getHeader(FIELD_COUNT);
            fields = new FieldPacket[FIELD_COUNT];
            header.packetId = ++packetId;
            for (int i = 1; i <= FIELD_COUNT; i++) {
                String colname = rset.getMetaData().getColumnName(i).toLowerCase();
                int colType = rset.getMetaData().getColumnType(i);
                switch (colType) {
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
                    ColMetaData colMetaData = new ColMetaData(colname, colType);
                    LOGGER.error(colMetaData.toString());
                }
            }

            eof.packetId = ++packetId;
            ByteBuffer buffer = c.allocate();
            /**
             *  write header
             **/
            buffer = header.write(buffer, c, true);

            /**
             * write fields
             */
            for (FieldPacket field : fields) {
                buffer = field.write(buffer, c, true);
            }
            /**
             * write eof
             */
            buffer = eof.write(buffer, c, true);

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
                            }
                            ;
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
            buffer = lastEof.write(buffer, c, true);

            /**
             * post write
             */
            c.write(buffer);

        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
            c.writeErrMessage(ErrorCode.ER_YES, e.getMessage());
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
                c.writeErrMessage(ErrorCode.ER_YES, e.getMessage());
            }
        }
        return;
    }
}
