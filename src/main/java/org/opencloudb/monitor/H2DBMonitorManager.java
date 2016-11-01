package org.opencloudb.monitor;

import org.h2.tools.Server;
import org.opencloudb.sqlfw.H2DBManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 监控服务对外提供统一的访问接口
 *
 * @author zagnix
 * @version 1.0
 * @create 2016-11-01 9:41
 */

public class H2DBMonitorManager {

    private final static Logger LOGGER = LoggerFactory.getLogger(H2DBManager.class);

    private Server server = null;
    private String port = "8082";
    private static String URL = "jdbc:h2:mem:db_moniter";

    private String user = "sa";
    private String password = "";

    private Connection h2DBMonitorConn;
    private static final H2DBMonitorManager h2DBMonitorManager = new H2DBMonitorManager();

    private H2DBMonitorManager(){
        startMonitorServer();
        init();
        String insertSql = "INSERT INTO t_memory VALUES(1, 'Hello','dd',12,1,22)";
        insert(getH2DBMonitorConn(),insertSql);
        insertSql = "INSERT INTO t_memory VALUES(2, 'World','dd',12,1,22)";
        insert(getH2DBMonitorConn(),insertSql);
    }

    private void init() {
        /**
         * 初始化数据库表
         */
        Connection conn = null;
        Statement stmt = null;
        String sql = null;

        try {

            try {
                Class.forName("org.h2.Driver");
            } catch (Exception e) {
                LOGGER.error(e.getMessage());
            }

            conn = DriverManager.getConnection(URL,user, password);
            /**
             * MyCat内存监控表
             * table:t_memory
             */
            sql = "CREATE TABLE t_memory(thread_id INT PRIMARY KEY,thread_name VARCHAR(255)," +
                         "memory_type VARCHAR(255),used BIGINT,free BIGINT,total BIGINT)";
            createH2dbTable(conn,sql,"t_memory");

            /**
             * MyCat 线程池监控
             * table:t_threadpool
             */
            sql = "CREATE TABLE t_threadpool(thread_name VARCHAR(255) PRIMARY KEY,pool_size BIGINT," +
                    "active_count INT,task_queue_size INT,completed_task INT,total_task INT)";
            createH2dbTable(conn,sql,"t_threadpool");


            /**
             * MyCat数据库连接池监控
             * table:t_connectpool
             */
            sql = "CREATE TABLE t_connectpool(processor VARCHAR(255) PRIMARY KEY,id INT," +
                    "mysqlId INT,host VARCHAR(32),port INT,l_port INT,net_in BIGINT," +
                    "net_out BIGINT,life BIGINT,closed VARCHAR(16),borrowed VARCHAR(16)," +
                    "SEND_QUEUE INT,schema VARCHAR(32),charset VARCHAR(32),txlevel VARCHAR(16)," +
                    "autocommit VARCHAR(16))";
            createH2dbTable(conn,sql,"t_connectpool");

            /**
             * MyCat Sql 统计
             * table: t_sqlstat
             */
            sql = "";
            createH2dbTable(conn,sql,"t_sqlstat");


        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                LOGGER.error(e.getMessage());
            }
        }

        h2DBMonitorConn = conn;
    }

    private void createH2dbTable(Connection conn,String ddl_sql,String tableName){

        if(tableName == null)
            return;

        if(ddl_sql == null)
            return;
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute("DROP TABLE IF EXISTS " + tableName);
            stmt.execute(ddl_sql);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
        }finally {
            try {
                if (stmt != null)
                    stmt.close();
            }catch (SQLException e){
                LOGGER.error(e.getMessage());
            }
        }
    }


    public void insert(Connection connection,String sql){
        if(sql == null)
            return;
        Statement statement = null;
        try {
            statement = h2DBMonitorConn.createStatement();
            statement.executeUpdate(sql);
        }catch (SQLException e){
            LOGGER.error(e.getMessage());
        }finally {
            try {
                if(statement != null){
                    statement.close();
                }
            }catch (SQLException e){
                LOGGER.error(e.getMessage());
            }
        }

    }


    private void startMonitorServer() {
        try {
            server = Server.createTcpServer(
                    new String[] { "-tcpPort", port }).start();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void stopMonitorServer() {
        if (server != null) {
            server.stop();
        }
    }




    public Connection getH2DBMonitorConn() {
        return h2DBMonitorConn;
    }

    public static H2DBMonitorManager getH2DBMonitorManager() {
        return h2DBMonitorManager;
    }
}
