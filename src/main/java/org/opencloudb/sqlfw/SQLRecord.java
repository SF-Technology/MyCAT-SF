package org.opencloudb.sqlfw;

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
    private long resultRows= 0L;
    private AtomicLong executionTimes= new AtomicLong(0);
    // last accessed time
    private long lastAccessedTimestamp= 0L;
    private long refCount = 0L;
    private long startTime = 0L;
    private long endTime = 0L;
    private final long ttl ;

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

    public long getRefCount() {
        return refCount;
    }

    public void setRefCount(long refCount) {
        this.refCount = refCount;
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


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SQLRecord sqlRecord = (SQLRecord) o;

        if (resultRows != sqlRecord.resultRows) return false;
        if (lastAccessedTimestamp != sqlRecord.lastAccessedTimestamp) return false;
        if (refCount != sqlRecord.refCount) return false;
        if (startTime != sqlRecord.startTime) return false;
        if (endTime != sqlRecord.endTime) return false;
        if (ttl != sqlRecord.ttl) return false;
        if (originalSQL != null ? !originalSQL.equals(sqlRecord.originalSQL) : sqlRecord.originalSQL != null)
            return false;
        if (modifiedSQL != null ? !modifiedSQL.equals(sqlRecord.modifiedSQL) : sqlRecord.modifiedSQL != null)
            return false;
        return executionTimes != null ? executionTimes.equals(sqlRecord.executionTimes) : sqlRecord.executionTimes == null;

    }



    @Override
    public int hashCode() {
        int result = originalSQL != null ? originalSQL.hashCode() : 0;
        result = 31 * result + (modifiedSQL != null ? modifiedSQL.hashCode() : 0);
        result = 31 * result + (int) (resultRows ^ (resultRows >>> 32));
        result = 31 * result + (executionTimes != null ? executionTimes.hashCode() : 0);
        result = 31 * result + (int) (lastAccessedTimestamp ^ (lastAccessedTimestamp >>> 32));
        result = 31 * result + (int) (refCount ^ (refCount >>> 32));
        result = 31 * result + (int) (startTime ^ (startTime >>> 32));
        result = 31 * result + (int) (endTime ^ (endTime >>> 32));
        result = 31 * result + (int) (ttl ^ (ttl >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "SQLRecord{" +
                "originalSQL='" + originalSQL + '\'' +
                ", modifiedSQL='" + modifiedSQL + '\'' +
                ", resultRows=" + resultRows +
                ", executionTimes=" + executionTimes +
                ", lastAccessedTimestamp=" + lastAccessedTimestamp +
                ", refCount=" + refCount +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", ttl=" + ttl +
                '}';
    }

    @Override
    public void update() {

        /**
         * 1.查询是已经存在条sql
         */
        boolean isAdd = true;
        final Connection h2DBConn = H2DBManager.getH2DBManager().getH2DBConn();;
        Statement stmt = null;
        ResultSet rset = null;


        try {

            String sql = "select original_sql from sql_record where original_sql = " + getOriginalSQL();

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("sql === >  " + sql);
            }

            stmt = h2DBConn.createStatement();
            rset = stmt.executeQuery(sql);
            if (rset.next()){
                isAdd = false;
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
            sql = "INSERT INTO sql_record VALUES('" + getOriginalSQL()+ "','" + getModifiedSQL() + "',"
                                                 + getResultRows() + "," + getExecutionTimes().get() + ","
                                                 + getStartTime() + "," + getEndTime() + ","
                                                 + getLastAccessedTimestamp() + ")";
        }else {
            sql = "UPDATE sql_record SET modified_sql ='" + getModifiedSQL() +"'," +
                                        "SET result_rows =" + getResultRows() + ","  +
                                        "SET exe_times =" + getExecutionTimes().get() + ","  +
                                        "SET start_time =" + getStartTime() + ","  +
                                        "SET end_time =" + getEndTime() + ","  +
                                        "SET lastaccess_t =" + getLastAccessedTimestamp() + ","  +
                                        "WHERE original_sql = " + getOriginalSQL();
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

    @Override
    public void insert() {

    }

    @Override
    public SQLRecord query(String key) {

        SQLRecord sqlRecord = new SQLRecord(SQLFirewallServer.DEFAULT_TIMEOUT);

        final Connection h2DBConn = H2DBManager.getH2DBManager().getH2DBConn();
        Statement stmt = null;
        ResultSet rset = null;

        try {
            String sql = "select * from sql_record where original_sql = " + key;

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("sql === >  " + sql);
            }

            stmt = h2DBConn.createStatement();
            rset = stmt.executeQuery(sql);

            while (rset.next()){
                sqlRecord.setOriginalSQL(rset.getString(1));
                sqlRecord.setModifiedSQL(rset.getString(2));
                sqlRecord.setResultRows(rset.getLong(3));
                sqlRecord.getExecutionTimes().set(rset.getLong(4));
                sqlRecord.setStartTime(rset.getLong(5));
                sqlRecord.setEndTime(rset.getLong(6));
                sqlRecord.setLastAccessedTimestamp(rset.getLong(7));
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
        final Connection h2DBConn = H2DBManager.getH2DBManager().getH2DBConn();
        Statement stmt = null;
        ResultSet rset = null;
        try {
            String sql = "DELETE FROM sql_record WHERE original_sql = '" + getOriginalSQL() + "'";

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
