package org.opencloudb.monitor;

/**
 * Mycat 监控内存表创建的SQL语句
 *
 * @author zagnix
 * @version 1.0
 * @create 2016-11-03 9:39
 */

public class TableCreateSQL {

    /**
     * T_MEMORY
     */
    public final static String   T_MEMORY = "CREATE TABLE t_memory(thread_id BIGINT PRIMARY KEY," +
            "thread_name VARCHAR(255)," +
            "memory_type VARCHAR(255)," +
            "used BIGINT," +
            "max BIGINT," +
            "total BIGINT)";

    /**
     * T_THREADPOOL
     */

    public final static String T_THREADPOOL = "CREATE TABLE t_threadpool(thread_name VARCHAR(255) PRIMARY KEY," +
            "pool_size BIGINT," +
            "active_count BIGINT," +
            "task_queue_size BIGINT," +
            "completed_task BIGINT," +
            "total_task BIGINT)";


    /**
     * t_connectpool
     */

    public final static String T_CONNCETPOOL =  "CREATE TABLE t_connectpool(processor VARCHAR(255)," +
            "id BIGINT PRIMARY KEY," +
            "mysqlId BIGINT," +
            "host VARCHAR(32)," +
            "port INT," +
            "l_port INT," +
            "net_in BIGINT," +
            "net_out BIGINT," +
            "life BIGINT," +
            "closed VARCHAR(16)," +
            "borrowed VARCHAR(16)," +
            "SEND_QUEUE INT," +
            "schema VARCHAR(32)," +
            "charset VARCHAR(32)," +
            "txlevel VARCHAR(16)," +
            "autocommit VARCHAR(16))";

    /**
     * t_sqlstat
     */

    public final static String T_SQLSTAT = "";

    /**
     * t_connection_cli
     */
    public final static String T_CONNECTION_CLI = "CREATE TABLE t_connection_cli(" +
            "id BIGINT PRIMARY KEY," +
            "processor VARCHAR(32)," +
            "host VARCHAR(32)," +
            "port INT," +
            "l_port INT," +
            "user VARCHAR(32)," +
            "schema VARCHAR(32)," +
            "charset VARCHAR(32)," +
            "net_in BIGINT," +
            "net_out BIGINT," +
            "alive_time BIGINT," +
            "recv_buffer BIGINT," +
            "send_queue BIGINT," +
            "txLevel VARCHAR(32)," +
            "autoCommit VARCHAR(32)" + ")";

    /**
     * t_database
     */
    public final static String T_DATABASE = "CREATE TABLE t_database(database VARCHAR(255) PRIMARY KEY)";


    /**
     * t_heartbeat
     */
    public final static String T_HEARTBEAT = "CREATE TABLE t_heartbeat( " +
            "name VARCHAR(32) PRIMARY KEY," +
            "type VARCHAR(16)," +
            "host VARCHAR(32)," +
            "port INT,"+
            "rs_code INT," +
            "retry INT," +
            "status VARCHAR(16)," +
            "timeout BIGINT," +
            "execute_time VARCHAR(128)," +
            "last_active_time VARCHAR(128)," +
            "stop VARCHAR(16),"+ ")";


    /**
     * t_datanode
     */
    public final static  String T_DATANODE="CREATE TABLE t_datanode(" +
            "name VARCHAR(32) PRIMARY KEY," +
            "datahost VARCHAR(32)," +
            "index INT," +
            "type VARCHAR(32)," +
            "active INT," +
            "idle INT," +
            "size INT," +
            "execute BIGINT," +
            "total_time DOUBLE," +
            "max_time DOUBLE," +
            "max_sql BIGINT," +
            "recovery_time BIGINT" + ")";

    /**
     * t_datasource
     */
    public static final String T_DATASOUCE = "CREATE TABLE t_datasource(" +
            "datanode VARCHAR(32) PRIMARY KEY," +
            "name VARCHAR(32)," +
            "type VARCHAR(32)," +
            "host VARCHAR(32)," +
            "port INT," +
            "W_R VARCHAR(16)," +
            "active INT," +
            "idle INT," +
            "size INT," +
            "execute BIGINT)";

    /**
     * t_cache
     */

    public static final  String T_CACHE = "CREATE TABLE t_cache(" +
            "cache VARCHAR(64) PRIMARY KEY," +
            "max BIGINT," +
            "cur BIGINT," +
            "access BIGINT," +
            "hit BIGINT," +
            "put BIGINT," +
            "last_access BIGINT," +
            "last_put BIGINT)";

    /**
     * t_processor
     */
    public static  final  String T_PROCESSOR = "CREATE TABLE t_processor(" +
            "name VARCHAR(32) PRIMARY KEY," +
            "net_in BIGINT," +
            "net_out BIGINT," +
            "reactor_count BIGINT," +
            "r_queue INT," +
            "w_queue INT," +
            "free_buffer BIGINT," +
            "total_buffer BIGINT," +
            "bu_percent INT," +
            "bu_warns INT," +
            "fc_count INT," +
            "bc_count INT)";


    /**
     * t_sysparam
     */
    public final static String T_SYSPARAM="CREATE TABLE t_sysparam(" +
            "var_name VARCHAR(32) PRIMARY KEY," +
            "var_value VARCHAR(32)," +
            "describe VARCHAR(255))";
}
