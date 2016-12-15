package org.opencloudb.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author zagnix
 * @create 2016-11-16 9:47
 */

public class SQLTypeSummary {

    private final static Logger LOGGER =
            LoggerFactory.getLogger(SQLTypeSummary.class);
    /**
     * pkey=sqlType+user+host+scheam+tables
     */
    private String pkey;
    private String sqlType;
    private String user;
    private String host;
    private String schema;
    private String tables;
    private long execSqlCount;
    private long execSqlTime;
    private long execSqlRows;

    public String getPkey() {
        return pkey;
    }

    public void setPkey(String pkey) {
        this.pkey = pkey;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTables() {
        return tables;
    }

    public void setTables(String tables) {
        this.tables = tables;
    }


    public String getSqlType() {
        return sqlType;
    }

    public void setSqlType(String sqlType) {
        this.sqlType = sqlType;
    }

    public long getExecSqlCount() {
        return execSqlCount;
    }

    public void setExecSqlCount(long execSqlCount) {
        this.execSqlCount = execSqlCount;
    }

    public long getExecSqlTime() {
        return execSqlTime;
    }

    public void setExecSqlTime(long execSqlTime) {
        this.execSqlTime = execSqlTime;
    }

    public long getExecSqlRows() {
        return execSqlRows;
    }

    public void setExecSqlRows(long execSqlRows) {
        this.execSqlRows = execSqlRows;
    }

    @Override
    public String toString() {
        return "SQLTypeSummary{" +
                "pkey='" + pkey + '\'' +
                ", sqlType='" + sqlType + '\'' +
                ", user='" + user + '\'' +
                ", host='" + host + '\'' +
                ", schema='" + schema + '\'' +
                ", tables='" + tables + '\'' +
                ", execSqlCount=" + execSqlCount +
                ", execSqlTime=" + execSqlTime +
                ", execSqlRows=" + execSqlRows +
                '}';
    }

    /**
     * 更新SQL Summary信息
     */
    public void update() {
        long execCount = 0;
        long execTime = 0;
        long exeCrows = 0;
        /**
         * 1.查询是已经存在条sql
         */
        boolean isAdd = true;
        final Connection h2DBConn =
                H2DBMonitorManager.getH2DBMonitorManager().getH2DBMonitorConn();
        Statement stmt = null;
        ResultSet rset = null;

        try {
            String sql = "select exec_count,exec_time,exec_rows from t_sqlsummary where pkey = '" + getPkey()+ "'";
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("sql === >  " + sql);
            }
            stmt = h2DBConn.createStatement();
            rset = stmt.executeQuery(sql);
            if (rset.next()){
                isAdd = false;
                execCount = rset.getLong(1);
                execTime = rset.getLong(2);
                exeCrows = rset.getLong(3);
            }
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
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
            }
        }

        /**
         * 2.根据1决定是添加还是更新
         */
        String sql = null;


        if(isAdd){
            sql = "INSERT INTO t_sqlsummary VALUES('" + getPkey()+ "','"
                    + getSqlType() + "','"
                    + getUser() + "','"
                    + getHost() + "','"
                    + getSchema() + "','"
                    + getTables() + "',"
                    + getExecSqlCount() + ","
                    + getExecSqlTime() + ","
                    + getExecSqlRows() + ")";
        }else {
            sql = "UPDATE t_sqlsummary SET  exec_count = " + (execCount + getExecSqlCount()) + ","  +
                    "exec_time =" + (execTime + getExecSqlTime()) + ","  +
                    "exec_rows =" +  (exeCrows + getExecSqlRows()) +
                    " WHERE pkey = '" + getPkey()+ "'";
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("sql === >  " + sql);
        }

        try {
            stmt = h2DBConn.createStatement();
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
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
            }
        }
    }


}
