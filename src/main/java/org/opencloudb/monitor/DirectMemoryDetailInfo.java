package org.opencloudb.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Mycat 堆外内存详细信息
 *
 * @author zagnix
 * @create 2017-5-19 17:08
 */

public class DirectMemoryDetailInfo {

    private final static Logger LOGGER =
            LoggerFactory.getLogger(DirectMemoryDetailInfo.class);

    private long threadId;
    private String memoryType;
    private long used;


    public long getThreadId() {
        return threadId;
    }

    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    public String getMemoryType() {
        return memoryType;
    }

    public void setMemoryType(String memoryType) {
        this.memoryType = memoryType;
    }

    public long getUsed() {
        return used;
    }

    public void setUsed(long used) {
        this.used = used;
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

            String sql = "select thread_id  from t_dmemory_detail where thread_id = " + getThreadId();

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
            sql = "INSERT INTO t_dmemory_detail VALUES(" + getThreadId() + ",'"
                    + getMemoryType() + "'," + getUsed() + ")";
        } else {
            sql = "UPDATE t_dmemory_detail SET used = " + getUsed() +
                    " WHERE thread_id = " + getThreadId();
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


    public static void delete(String memoryType) {

        if (memoryType == null)
            return;
        final Connection h2DBConn =
                H2DBMonitorManager.getH2DBMonitorManager().getH2DBMonitorConn();
        Statement stmt = null;

        try {
            String sql = "UPDATE t_dmemory_detail SET used = " + 0 +
                    " where memory_type ='" + memoryType.trim() + "'";

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
            } catch (SQLException e) {
                LOGGER.error(e.getMessage());
            }
        }

    }
}
