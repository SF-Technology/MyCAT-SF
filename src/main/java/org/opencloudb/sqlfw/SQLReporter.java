package org.opencloudb.sqlfw;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 记录对拦截的SQL语句信息
 *
 * @author zagnix
 * @version 1.0
 * @create 2016-10-25 9:23
 */

public class SQLReporter implements H2DBInterface<SQLReporter> {
    private final static Logger LOGGER =
            LoggerFactory.getLogger(SQLReporter.class);

    private String sql;
    private String sqlMsg;
    private int count;

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getSqlMsg() {
        return sqlMsg;
    }

    public void setSqlMsg(String sqlMsg) {
        this.sqlMsg = sqlMsg;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SQLReporter that = (SQLReporter) o;

        if (count != that.count) return false;
        if (sql != null ? !sql.equals(that.sql) : that.sql != null) return false;
        return sqlMsg != null ? sqlMsg.equals(that.sqlMsg) : that.sqlMsg == null;

    }

    @Override
    public int hashCode() {
        int result = sql != null ? sql.hashCode() : 0;
        result = 31 * result + (sqlMsg != null ? sqlMsg.hashCode() : 0);
        result = 31 * result + count;
        return result;
    }

    @Override
    public String toString() {
        return "SQLReporter{" +
                "sql='" + sql + '\'' +
                ", sqlMsg='" + sqlMsg + '\'' +
                ", count=" + count +
                '}';
    }

    @Override
    public void update() {

        /**
         * 1.查询是已经存在条sql
         */
        boolean isAdd = true;
        int sqlCount = 0;
        final Connection h2DBConn = H2DBManager.getH2DBManager().getH2DBConn();
        Statement stmt = null;
        ResultSet rset = null;

        try {
            String sql = "select sql,count from t_sqlreporter where sql = '" + getSql() + "'";
            stmt = h2DBConn.createStatement();
            rset = stmt.executeQuery(sql);
            if (rset.next()) {
                sqlCount = rset.getInt(2)+1;
                isAdd = false;
            }
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

        /**
         * 2.根据1决定是添加还是更新
         */
        String sql = null;

        if (isAdd) {
            sql = "INSERT INTO t_sqlreporter VALUES('" + getSql() + "','" + getSqlMsg() + "'," + sqlCount +")";
        } else {
            sql = "UPDATE t_sqlreporter SET count= " + sqlCount + " WHERE sql = '" + getSql() + "'";
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("sql === >  " + sql);
        }

        try {
            stmt = h2DBConn.createStatement();
            stmt.executeUpdate(sql);
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

    @Override
    public void update_row() {

    }

    @Override
    public void insert() {

    }

    @Override
    public SQLReporter query(String key) {
        return null;
    }

    @Override
    public void delete() {
        final Connection h2DBConn = H2DBManager.getH2DBManager().getH2DBConn();
        Statement stmt = null;
        ResultSet rset = null;
        try {
            String sql = "DELETE FROM t_sqlreporter WHERE sql = '" + getSql() + "'";
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
