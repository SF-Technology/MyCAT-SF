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
 * SQL History Record
 *
 * @author zagnix
 * @create 2016-10-20 13:44
 */
public class SQLHistoryRecord implements H2DBInterface<SQLHistoryRecord> {
    private final static Logger LOGGER =
            LoggerFactory.getLogger(SQLHistoryRecord.class);
    private String originalSQL = null;
    private String modifiedSQL = "sql no change";
    private String user = null;
    private String host = null;
    private String schema = null;
    private String tables = null;
    private int sqlType = 0;
    private long resultRows= 0L;
    private AtomicLong executionTimes= new AtomicLong(0);
    private long lastAccessedTimestamp= 0L;
    private long startTime = 0L;
    private long endTime = 0L;
    private long sqlExecTime = 0L;
    public SQLHistoryRecord(){
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
        this.modifiedSQL = modifiedSQL!=null?this.modifiedSQL:null;
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

    public long getLastAccessedTimestamp() {
        return lastAccessedTimestamp;
    }

    public void setLastAccessedTimestamp(long lastAccessedTimestamp) {
        this.lastAccessedTimestamp = lastAccessedTimestamp;
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

    public String getTables() {
        return tables;
    }

    public void setTables(String tables) {
        this.tables = tables;
    }

    public int getSqlType() {
        return sqlType;
    }

    public void setSqlType(int sqlType) {
        this.sqlType = sqlType;
    }

    @Override
    public void update() {
        final Connection h2DBConn =
                H2DBManager.getH2DBManager().getH2DBConn();
        Statement stmt = null;
        ResultSet rset = null;
        String sql = null;
        sql = "INSERT INTO t_sqlrecord VALUES('" + getOriginalSQL().replace("'","")+ "','" +
                getModifiedSQL() + "','"
                + getUser() + "','"
                + getHost() + "','"
                + getSchema() + "','"
                + getTables() + "',"
                + getSqlType() + ","
                + getResultRows() + ","
                + getExecutionTimes().get() + ","
                + getStartTime() + ","
                + getEndTime() + ","
                + getSqlExecTime() + ","
                + getLastAccessedTimestamp() + ")";

        LOGGER.error(sql);
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

    }

    @Override
    public void insert() {

    }

    @Override
    public SQLHistoryRecord query(String key) {
        return null;
    }

    @Override
    public void delete() {

    }
}
