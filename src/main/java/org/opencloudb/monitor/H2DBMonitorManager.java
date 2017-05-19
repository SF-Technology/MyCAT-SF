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
        //startMonitorServer();
        init();
    }

    private void init() {
        /**
         * 初始化数据库表
         */
        Connection conn = null;
        Statement stmt = null;

        try {

            try {
                Class.forName("org.h2.Driver");
            } catch (Exception e) {
                LOGGER.error(e.getMessage());
            }

            conn = DriverManager.getConnection(URL,user, password);

            /**
             * MyCat内存监控表
             * table:t_dmemory
             */
            createH2dbTable(conn,TableCreateSQL.T_DIRECT_MEMORY,"t_dmemory");

            /**
             * MyCat内存监控表
             * table:t_dmemory_detail
             */
            createH2dbTable(conn,TableCreateSQL.T_DMEMORY_DETAIL,"t_dmemory_detail");

            /**
             * MyCat内存监控表
             * table:t_memory
             */
            createH2dbTable(conn,TableCreateSQL.T_MEMORY,"t_memory");
            /**
             * MyCat 线程池监控
             * table:t_threadpool
             */
            createH2dbTable(conn,TableCreateSQL.T_THREADPOOL,"t_threadpool");
            /**
             * MyCat数据库连接池监控
             * table:t_connectpool
             */
            createH2dbTable(conn,TableCreateSQL.T_CONNCETPOOL,"t_connectpool");

            /**
             * MyCat Sql 统计
             * table: t_sqlstat
             */

            createH2dbTable(conn,TableCreateSQL.T_SQLSTAT,"t_sqlstat");

            /**
             * 用户连接信息
             * table:t_connection_cli
             */
            createH2dbTable(conn,TableCreateSQL.T_CONNECTION_CLI,"t_connection_cli");


            /**
             * db 信息
             * table:t_database
             */
            createH2dbTable(conn,TableCreateSQL.T_DATABASE,"t_database");


            /**
             * heartbeat 信息
             * table :t_heartbeat
             */
            createH2dbTable(conn,TableCreateSQL.T_HEARTBEAT,"t_heartbeat");

            /**
             * datanode 信息
             * table:t_datanode
             */
            createH2dbTable(conn,TableCreateSQL.T_DATANODE,"t_datanode");


            /**
             * datasource 信息
             * table:t_datasource
             */
            createH2dbTable(conn,TableCreateSQL.T_DATASOUCE,"t_datasource");


            /**
             * Cache 信息
             * table: t_cache
             */
            createH2dbTable(conn,TableCreateSQL.T_CACHE,"t_cache");

            /**
             * processor 信息
             * table:t_processor
             */
            createH2dbTable(conn,TableCreateSQL.T_PROCESSOR,"t_process");


            /**
             * sys param 信息
             * table:t_sysparam
             */
            createH2dbTable(conn,TableCreateSQL.T_SYSPARAM,"t_sysparam");

            /**
             * sql summary 信息
             * table:t_sqlsummary
             */
            createH2dbTable(conn,TableCreateSQL.T_SQLSUMMARY,"t_sqlsummary");


            /**
             * topN exec time 信息
             * table:t_topntime
             */
            createH2dbTable(conn,TableCreateSQL.T_TOPNTIME,"t_topntime");


            /**
             * topN exec count 信息
             * table:t_topncount
             */
            createH2dbTable(conn,TableCreateSQL.T_TOPNCOUNT,"t_topncount");


            /**
             * topN exec rows
             * table:t_topnrows
             */
            createH2dbTable(conn,TableCreateSQL.T_TOPNROWS,"t_topnrows");

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
