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
     * T_DMEMORY
     */
    public final static String T_DIRECT_MEMORY = "CREATE TABLE t_dmemory(id INT PRIMARY KEY," +
            "memory_type VARCHAR(64)," +
            "used BIGINT," +
            "max BIGINT," +
            "total BIGINT)";

    /**
     * T_DMEMORY_DETAIL
     */
    public final static String T_DMEMORY_DETAIL = "CREATE TABLE t_dmemory_detail(thread_id BIGINT PRIMARY KEY," +
            "memory_type VARCHAR(64)," +
            "used BIGINT)";

    /**
     * T_MEMORY
     */
    public final static String T_MEMORY = "CREATE TABLE t_memory(thread_id BIGINT PRIMARY KEY," +
            "thread_name VARCHAR(64)," +
            "memory_type VARCHAR(64)," +
            "used BIGINT," +
            "max BIGINT," +
            "total BIGINT)";

    /**
     * T_THREADPOOL
     */

    public final static String T_THREADPOOL = "CREATE TABLE t_threadpool(" +
            "thread_name VARCHAR(255) PRIMARY KEY," +
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

    public final static String T_SQLSTAT= "CREATE TABLE t_sqlstat(" +
            "original_sql VARCHAR(2048) PRIMARY KEY, " +
            "modified_sql VARCHAR(2048)," +
            "user VARCHAR(32)," +
            "host VARCHAR(64)," +
            "schema VARCHAR(32)," +
            "tables VARCHAR(32)," +
            "sqltype INT," +
            "result_rows BIGINT," +
            "exe_times BIGINT," +
            "start_time BIGINT," +
            "end_time BIGINT," +
            "sqlexec_time BIGINT," +
            "lastaccess_t BIGINT)";


    /**
     * t_sqlrecord
     */
    public final static String T_SQLRECORD= "CREATE TABLE t_sqlrecord(" +
            "original_sql VARCHAR(2048) PRIMARY KEY," +
            "modified_sql VARCHAR(2048)," +
            "user VARCHAR(32)," +
            "host VARCHAR(64)," +
            "schema VARCHAR(32)," +
            "tables VARCHAR(32)," +
            "sqltype INT," +
            "result_rows BIGINT," +
            "exe_times BIGINT," +
            "start_time BIGINT," +
            "end_time BIGINT," +
            "sqlexec_time BIGINT," +
            "lastaccess_t BIGINT )";



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
            "host VARCHAR(255) PRIMARY KEY," +
            "type VARCHAR(16)," +
            "name VARCHAR(128)," +
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
            "var_value VARCHAR(255)," +
            "describe VARCHAR(255))";


    /**
     * sql_blacklist
     */
    public final static String T_SQLBLACKLIST = "CREATE TABLE sql_blacklist(" +
            "sql_id INT auto_increment PRIMARY KEY, " +
            "sql VARCHAR(2048))";

    /**
     * sql_reporter
     */
    public final static String  T_SQLREPORTER = "CREATE TABLE sql_reporter(" +
            "sql VARCHAR(2048) PRIMARY KEY, " +
            "sql_msg VARCHAR(512)," +
            "count INT);";


    /**
     * t_sqlsummary
     */
    public final static String  T_SQLSUMMARY = "CREATE TABLE t_sqlsummary(" +
            "pkey VARCHAR(255) PRIMARY KEY, " +
            "sql_type VARCHAR(16)," +
            "user VARCHAR(32)," +
            "host VARCHAR(32)," +
            "schema VARCHAR(32)," +
            "tables VARCHAR(255)," +
            "exec_count BIGINT," +
            "exec_time BIGINT," +
            "exec_rows BIGINT);";

    /**
     * t_topnrows
     */
    public final static String  T_TOPNROWS= "CREATE TABLE t_topnrows(" +
            "sql VARCHAR(2048) PRIMARY KEY, " +
            "user VARCHAR(32)," +
            "host VARCHAR(32)," +
            "schema VARCHAR(32)," +
            "tables VARCHAR(255)," +
            "exec_rows BIGINT);";

    /**
     * t_topntime
     */
    public final static String  T_TOPNTIME= "CREATE TABLE t_topntime(" +
            "sql VARCHAR(2048) PRIMARY KEY, " +
            "user VARCHAR(32)," +
            "host VARCHAR(32)," +
            "schema VARCHAR(32)," +
            "tables VARCHAR(255)," +
            "exec_time BIGINT);";

    /**
     * t_topncount
     */
    public final static String  T_TOPNCOUNT= "CREATE TABLE t_topncount(" +
            "sql VARCHAR(2048) PRIMARY KEY, " +
            "user VARCHAR(32)," +
            "host VARCHAR(32)," +
            "schema VARCHAR(32)," +
            "tables VARCHAR(255)," +
            "exec_count BIGINT);";

    /**
     * 监控表结构一致性的记录表
     * t_tsc
     */
    public final static String T_TSC = "CREATE TABLE t_tsc(" +
            "schema VARCHAR(64) PRIMARY KEY, " +
            "consistency VARCHAR(32)," +
            "desc VARCHAR(2048));";
}
