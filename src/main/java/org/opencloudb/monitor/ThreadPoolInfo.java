package org.opencloudb.monitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * @author zagnix
 * @create 2016-11-02 15:46
 */

public class ThreadPoolInfo {

    private final static Logger LOGGER =
            LoggerFactory.getLogger(ThreadPoolInfo.class);

    private String threadName;
    private long poolSize;
    private int activeCount;
    private int taskQueueSize;
    private long compeletedTask;
    private long totalTask;

    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    public long getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(long poolSize) {
        this.poolSize = poolSize;
    }

    public int getActiveCount() {
        return activeCount;
    }

    public void setActiveCount(int activeCount) {
        this.activeCount = activeCount;
    }

    public int getTaskQueueSize() {
        return taskQueueSize;
    }

    public void setTaskQueueSize(int taskQueueSize) {
        this.taskQueueSize = taskQueueSize;
    }

    public long getCompeletedTask() {
        return compeletedTask;
    }

    public void setCompeletedTask(long compeletedTask) {
        this.compeletedTask = compeletedTask;
    }

    public long getTotalTask() {
        return totalTask;
    }

    public void setTotalTask(long totalTask) {
        this.totalTask = totalTask;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ThreadPoolInfo that = (ThreadPoolInfo) o;

        if (poolSize != that.poolSize) return false;
        if (activeCount != that.activeCount) return false;
        if (taskQueueSize != that.taskQueueSize) return false;
        if (compeletedTask != that.compeletedTask) return false;
        if (totalTask != that.totalTask) return false;
        return threadName != null ? threadName.equals(that.threadName) : that.threadName == null;

    }

    @Override
    public int hashCode() {
        int result = threadName != null ? threadName.hashCode() : 0;
        result = 31 * result + (int) (poolSize ^ (poolSize >>> 32));
        result = 31 * result + activeCount;
        result = 31 * result + taskQueueSize;
        result = 31 * result + (int) (compeletedTask ^ (compeletedTask >>> 32));
        result = 31 * result + (int) (totalTask ^ (totalTask >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "ThreadPoolInfo{" +
                "threadName='" + threadName + '\'' +
                ", poolSize=" + poolSize +
                ", activeCount=" + activeCount +
                ", taskQueueSize=" + taskQueueSize +
                ", compeletedTask=" + compeletedTask +
                ", totalTask=" + totalTask +
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

            String sql = "select thread_name from t_threadpool where thread_name = '" + getThreadName() + "'";

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

        /**
         * CREATE TABLE t_threadpool(thread_name VARCHAR(255) PRIMARY KEY,pool_size BIGINT," +
         "active_count INT,task_queue_size INT,completed_task INT,total_task INT)
         */

        if(isAdd){
            sql = "INSERT INTO t_threadpool VALUES('" + getThreadName()+ "'," + getPoolSize() + ","
                    + getActiveCount() + "," + getTaskQueueSize() + ","
                    + getCompeletedTask() + "," + getTotalTask() +")";
        }else {
            sql = "UPDATE t_threadpool SET pool_size =" +getPoolSize() +"," +
                    "active_count =" + getActiveCount() + ","  +
                    "task_queue_size = " + getTaskQueueSize() + ","  +
                    "completed_task = " + getCompeletedTask() + ","  +
                    "total_task =" + getTotalTask()+
                    " WHERE thread_name = '" + getThreadName() + "'";
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
