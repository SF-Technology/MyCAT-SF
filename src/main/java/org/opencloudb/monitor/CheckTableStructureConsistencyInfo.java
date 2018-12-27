package org.opencloudb.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 记录 schema 表结构一致性类
 *
 * @author zagnix
 * @create 2017-05-22 16:58
 */

public class CheckTableStructureConsistencyInfo {
    private final static Logger LOGGER =
            LoggerFactory.getLogger(SQLRecord.class);
    private String schemaName;
    private String consistency;
    private String desc;

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getConsistency() {
        return consistency;
    }

    public void setConsistency(String consistency) {
        this.consistency = consistency;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }


    @Override
    public String toString() {
        return "CheckTableStructureConsistencyInfo{" +
                "schemaName='" + schemaName + '\'' +
                ", consistency='" + consistency + '\'' +
                ", desc='" + desc + '\'' +
                '}';
    }



    public void update() {

        /**
         * 1.查询是已经存在
         */
        boolean isAdd = true;
        final Connection h2DBConn =
                H2DBMonitorManager.getH2DBMonitorManager().getH2DBMonitorConn();
        Statement stmt = null;
        ResultSet rset = null;

        try {

            String sql = "select schema from t_tsc where schema = '" + getSchemaName() + "'";
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("sql === >  " + sql);
            }
            stmt = h2DBConn.createStatement();
            rset = stmt.executeQuery(sql);
            if (rset.next()) {
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
            sql = "INSERT INTO t_tsc VALUES('" + getSchemaName() + "','" + getConsistency() + "','"
                    + getDesc() + "')";
        } else {
            sql = "UPDATE t_tsc SET consistency ='" + getConsistency() + "'," +   "desc = '" + getDesc() + "'"  +
                    " WHERE schema = '" + getSchemaName() +"'";
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
}
