package org.opencloudb.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Mycat 内存信息
 *
 * @author zagnix
 * @create 2016-11-02 14:08
 */

public class MemoryInfo {

    private final static Logger LOGGER =
            LoggerFactory.getLogger(MemoryInfo.class);

    private long threadId;
    private String threadName;
    private String memoryType;
    private long used;
    private long max;
    private long total;

    public long getMax() {
        return max;
    }

    public void setMax(long max) {
        this.max = max;
    }

    public long getThreadId() {
        return threadId;
    }

    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
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


    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MemoryInfo that = (MemoryInfo) o;

        if (threadId != that.threadId) return false;
        if (used != that.used) return false;
        if (max != that.max) return false;
        if (total != that.total) return false;
        if (threadName != null ? !threadName.equals(that.threadName) : that.threadName != null) return false;
        return memoryType != null ? memoryType.equals(that.memoryType) : that.memoryType == null;
    }

    @Override
    public int hashCode() {
        int result = (int) (threadId ^ (threadId >>> 32));
        result = 31 * result + (threadName != null ? threadName.hashCode() : 0);
        result = 31 * result + (memoryType != null ? memoryType.hashCode() : 0);
        result = 31 * result + (int) (used ^ (used >>> 32));
        result = 31 * result + (int) (max ^ (max >>> 32));
        result = 31 * result + (int) (total ^ (total >>> 32));
        return result;
    }


    @Override
    public String toString() {
        return "MemoryInfo{" +
                "threadId=" + threadId +
                ", threadName='" + threadName + '\'' +
                ", memoryType='" + memoryType + '\'' +
                ", used=" + used +
                ", max=" + max +
                ", total=" + total +
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

            String sql = "select thread_id  from t_memory where thread_id = " + getThreadId();

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
            sql = "INSERT INTO t_memory VALUES(" + getThreadId()+ ",'" + getThreadName() + "','"
                    + getMemoryType() + "'," + getUsed() + ","
                    + getMax() + "," + getTotal() +")";
        }else {
            sql = "UPDATE t_memory SET thread_name ='" + getThreadName() +"'," +
                    "memory_type ='" + getMemoryType() + "',"  +
                    "used = " + getUsed() + ","  +
                    "max = " + getMax() + ","  +
                    "total =" + getTotal()+
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
