package org.opencloudb.monitor;

import org.opencloudb.sqlfw.H2DBInterface;
import org.opencloudb.sqlfw.H2DBManager;
import org.opencloudb.sqlfw.SQLFirewallServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SQL Record
 *
 * @author zagnix
 * @create 2016-10-20 13:44
 */
public class SQLRecord implements H2DBInterface<SQLRecord> {

    private final static Logger LOGGER =
            LoggerFactory.getLogger(SQLRecord.class);

    private String originalSQL = null;
    private String modifiedSQL = null;
    private String user = null;
    private String host = null;
    private String schema = null;
    private long resultRows= 0L;
    private AtomicLong executionTimes= new AtomicLong(0);
    private long lastAccessedTimestamp= 0L;
    private long startTime = 0L;
    private long endTime = 0L;
    private final long ttl ;
    private long sqlExecTime = 0L;
    public SQLRecord(final long timeout){
        this.ttl = timeout;
    }
    public String getOriginalSQL() {
        return originalSQL;
    }

    public void setOriginalSQL(String originalSQL) {
        this.originalSQL = originalSQL;
    }

    public String getModifiedSQL() {
        return modifiedSQL;
    }

    public void setModifiedSQL(String modifiedSQL) {
        this.modifiedSQL = modifiedSQL;
    }

    public long getResultRows() {
        return resultRows;
    }

    public void setResultRows(long resultRows) {
        this.resultRows = resultRows;
    }

    public AtomicLong getExecutionTimes() {
        return executionTimes;
    }

    public void setExecutionTimes(AtomicLong executionTimes) {
        this.executionTimes = executionTimes;
    }


    public long getLastAccessedTimestamp() {
        return lastAccessedTimestamp;
    }

    public void setLastAccessedTimestamp(long lastAccessedTimestamp) {
        this.lastAccessedTimestamp = lastAccessedTimestamp;
    }

    public long getTtl() {
        return ttl;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
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
    public long getSqlExecTime() {
        return sqlExecTime;
    }

    public void setSqlExecTime(long sqlExecTime) {
        this.sqlExecTime = sqlExecTime;
    }

    @Override
    public String toString() {
        return "SQLRecord{" +
                "originalSQL='" + originalSQL + '\'' +
                ", modifiedSQL='" + modifiedSQL + '\'' +
                ", user='" + user + '\'' +
                ", host='" + host + '\'' +
                ", schema='" + schema + '\'' +
                ", resultRows=" + resultRows +
                ", executionTimes=" + executionTimes +
                ", lastAccessedTimestamp=" + lastAccessedTimestamp +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", ttl=" + ttl +
                ", sqlExecTime=" + sqlExecTime +
                '}';
    }

    @Override
    public void update() {

        /**
         * 1.查询是已经存在条sql
         */
        boolean isAdd = true;
        final Connection h2DBConn =
                H2DBMonitorManager.getH2DBMonitorManager().getH2DBMonitorConn();
        Statement stmt = null;
        ResultSet rset = null;
        long exe_times = 0;

        try {
            String sql = "select original_sql,exe_times from t_sqlstat where original_sql = '" + getOriginalSQL().replace("'","") + "'";
            LOGGER.info("sql === >  " + sql);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("sql === >  " + sql);
            }
            stmt = h2DBConn.createStatement();
            rset = stmt.executeQuery(sql);
            if (rset.next()){
                isAdd = false;
                exe_times = rset.getLong(2);
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
            sql = "INSERT INTO t_sqlstat VALUES('" + getOriginalSQL().replace("'","")+ "','" +
                    getModifiedSQL() + "','"
                    + getUser() + "','"
                    + getHost() + "','"
                    + getSchema() + "',"
                    + getResultRows() + ","
                    + getExecutionTimes().get() + ","
                    + getStartTime() + ","
                    + getEndTime() + ","
                    + getSqlExecTime() + ","
                    + getLastAccessedTimestamp() + ")";

        }else {
            sql = "UPDATE t_sqlstat SET modified_sql ='" + getModifiedSQL().replace("'","") +"'," +
                                        "user ='" + getUser() + "',"  +
                                        "host ='" + getHost() + "',"  +
                                        "schema ='" + getSchema() + "',"  +
                                        "result_rows =" + getResultRows() + ","  +
                                        "exe_times =" + (exe_times+1) + ","  +
                                        "start_time =" + getStartTime() + ","  +
                                        "end_time =" + getEndTime() + "," +
                                        "sqlexec_time =" + getSqlExecTime() +
                                        " WHERE original_sql = '" + getOriginalSQL().replace("'","") + "'";
        }

        LOGGER.info("sql === >  " + sql);
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

    @Override
    public void update_row() {
        final Connection h2DBConn =
                H2DBMonitorManager.getH2DBMonitorManager().getH2DBMonitorConn();
        Statement stmt = null;
        ResultSet rset = null;
        String  sql = "UPDATE t_sqlstat SET result_rows = " + getResultRows() + " WHERE original_sql = '" + getOriginalSQL().replace("'","") + "'";
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

    @Override
    public void insert() {

    }

    @Override
    public SQLRecord query(String key) {

        SQLRecord sqlRecord = new SQLRecord(SQLFirewallServer.DEFAULT_TIMEOUT);

        final Connection h2DBConn =
                H2DBMonitorManager.getH2DBMonitorManager().getH2DBMonitorConn();
        Statement stmt = null;
        ResultSet rset = null;

        try {
            String sql = "select * from t_sqlstat where original_sql = '" + key.replace("'","") + "'";

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("sql === >  " + sql);
            }

            stmt = h2DBConn.createStatement();
            rset = stmt.executeQuery(sql);

            while (rset.next()){
                sqlRecord.setOriginalSQL(rset.getString(1));
                sqlRecord.setModifiedSQL(rset.getString(2));
                sqlRecord.setUser(rset.getString(3));
                sqlRecord.setHost(rset.getString(4));
                sqlRecord.setSchema(rset.getString(5));
                sqlRecord.setResultRows(rset.getLong(6));
                sqlRecord.getExecutionTimes().set(rset.getLong(7));
                sqlRecord.setStartTime(rset.getLong(8));
                sqlRecord.setEndTime(rset.getLong(9));
                sqlRecord.setSqlExecTime(rset.getLong(10));
                sqlRecord.setLastAccessedTimestamp(rset.getLong(11));
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

        return sqlRecord;
    }

    @Override
    public void delete() {
        final Connection h2DBConn =
                H2DBMonitorManager.getH2DBMonitorManager().getH2DBMonitorConn();
        Statement stmt = null;
        ResultSet rset = null;
        try {
            String sql = "DELETE FROM t_sqlstat WHERE original_sql = '" + getOriginalSQL().replace("'","") + "'";
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("sql === >  " + sql);
            }
            stmt = h2DBConn.createStatement();
            stmt.execute(sql);
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
    }
}
