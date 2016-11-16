package org.opencloudb.sqlfw;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.monitor.TableCreateSQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * H2DB Server Manager
 *
 * @author zagnix
 * @version 1.0
 * @create 2016-10-20 16:13
 */

public class H2DBManager {

    private final static Logger LOGGER = LoggerFactory.getLogger(H2DBManager.class);
    private static final String h2dbURI = "jdbc:h2:"+SystemConfig.getHomePath()+"/h2db/db_sqlfw";// H2 database;
    private static final String dbName = "db_sqlfw";
    private static  final String sqlRecordTableName = "t_sqlrecord";
    private static  final String sqlBackListTableName = "t_sqlblacklist";
    private static final String sqlReporterTableName = "t_sqlreporter";
    private static  final String user = "sa";
    private static  final String key = "";
    private  Connection h2DBConn;

    private static final H2DBManager h2DBManager = new H2DBManager();

    private H2DBManager() {
        init();
    }

    /**
     * 初始化 H2DB 中的 db_sqlfw数据连接
     */
    private void init(){
        Connection conn = null;
        Statement stmt = null;
        try {
            try {
                Class.forName("org.h2.Driver");// H2 Driver
            } catch (Exception e) {
                e.printStackTrace();
            }
            conn = DriverManager.getConnection(h2dbURI, user, key);


            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("show tables");


            if(!rs.next()){
                stmt.execute(TableCreateSQL.T_SQLBLACKLIST);
                stmt.execute(TableCreateSQL.T_SQLREPORTER);
                stmt.execute(TableCreateSQL.T_SQLRECORD);
            }

            rs.close();
            rs.close();

            rs = stmt.executeQuery("select * from t_sqlblacklist limit 1");

            if (!rs.next()){
                stmt.execute("DROP TABLE IF EXISTS t_sqlblacklist");
                stmt.execute(TableCreateSQL.T_SQLBLACKLIST);
            }

            rs.close();
            rs = stmt.executeQuery("select * from t_sqlreporter limit 1");
            if (!rs.next()){
                stmt.execute("DROP TABLE IF EXISTS t_sqlreporter");
                stmt.execute(TableCreateSQL.T_SQLREPORTER);
            }
            rs.close();

            rs = stmt.executeQuery("select * from t_sqlrecord limit 1");

            if (!rs.next()){
                stmt.execute("DROP TABLE IF EXISTS t_sqlrecord");
                stmt.execute(TableCreateSQL.T_SQLRECORD);
            }

            rs.close();

        }catch (SQLException sqle) {
            LOGGER.error(sqle.getMessage());
        }finally {
                try {
                    if(stmt != null) {
                        stmt.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
        }

        h2DBConn = conn;
    }


    public static H2DBManager getH2DBManager() {
        return h2DBManager;
    }

    /**
     * 返回H2DB一个数据库连接
     * @return
     */

    public  Connection getH2DBConn() {
        return h2DBConn;
    }
    public static String getSqlRecordTableName() {
        return sqlRecordTableName;
    }
    public static  String getSqlBackListTableName() {
        return sqlBackListTableName;
    }
    public static String getSqlReporterTableName() {
        return sqlReporterTableName;
    }

    /**
     * 关闭H2DB数据库连接
     */
    public  void closeH2DBCon(){
        if (h2DBConn != null){
            try {
                h2DBConn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    /**
     * 执行H2DB数据库删除操作
     * @param sql
     * @return
     */
    public boolean delete(String sql){
        boolean flag = false;
        try {
            Statement stmt = h2DBConn.createStatement();
            flag = stmt.execute(sql);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return flag;
    }
}
