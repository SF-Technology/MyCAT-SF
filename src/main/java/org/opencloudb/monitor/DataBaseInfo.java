package org.opencloudb.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author zagnix
 * @create 2016-11-03 11:05
 */

public class DataBaseInfo {

    private final static Logger LOGGER =
            LoggerFactory.getLogger(DataBaseInfo.class);

    private String dbName;

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataBaseInfo that = (DataBaseInfo) o;

        return dbName != null ? dbName.equals(that.dbName) : that.dbName == null;

    }

    @Override
    public int hashCode() {
        return dbName != null ? dbName.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "DatabaseInfo{" +
                "dbName='" + dbName + '\'' +
                '}';
    }


    public void update() {

        final Connection h2DBConn =
                H2DBMonitorManager.getH2DBMonitorManager().getH2DBMonitorConn();
        Statement stmt = null;
        String sql = "INSERT INTO t_database VALUES('" +getDbName()+"')";
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

            } catch (SQLException e) {
                LOGGER.error(e.getMessage());
            }
        }
    }
}
