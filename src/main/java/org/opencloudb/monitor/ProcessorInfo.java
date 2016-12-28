package org.opencloudb.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author zagnix
 * @create 2016-11-03 16:47
 */

public class ProcessorInfo {

    private final static Logger LOGGER =
            LoggerFactory.getLogger(DataNodeInfo.class);

    private String name;
    private long netIN;
    private long netOut;
    private long reactorCount;
    private int rQueue;
    private int wQueue;
    private long freeBuffer;
    private long totalBuffer;
    private int bufferPercent;
    private int bufferWarns;
    private int fcCount;
    private int bcCount;



    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getNetIN() {
        return netIN;
    }

    public void setNetIN(long netIN) {
        this.netIN = netIN;
    }

    public long getNetOut() {
        return netOut;
    }

    public void setNetOut(long netOut) {
        this.netOut = netOut;
    }

    public long getReactorCount() {
        return reactorCount;
    }

    public void setReactorCount(long reactorCount) {
        this.reactorCount = reactorCount;
    }

    public int getrQueue() {
        return rQueue;
    }

    public void setrQueue(int rQueue) {
        this.rQueue = rQueue;
    }

    public int getwQueue() {
        return wQueue;
    }

    public void setwQueue(int wQueue) {
        this.wQueue = wQueue;
    }

    public long getFreeBuffer() {
        return freeBuffer;
    }

    public void setFreeBuffer(long freeBuffer) {
        this.freeBuffer = freeBuffer;
    }

    public long getTotalBuffer() {
        return totalBuffer;
    }

    public void setTotalBuffer(long totalBuffer) {
        this.totalBuffer = totalBuffer;
    }

    public int getBufferPercent() {
        return bufferPercent;
    }

    public void setBufferPercent(int bufferPercent) {
        this.bufferPercent = bufferPercent;
    }

    public int getBufferWarns() {
        return bufferWarns;
    }

    public void setBufferWarns(int bufferWarns) {
        this.bufferWarns = bufferWarns;
    }

    public int getFcCount() {
        return fcCount;
    }

    public void setFcCount(int fcCount) {
        this.fcCount = fcCount;
    }

    public int getBcCount() {
        return bcCount;
    }

    public void setBcCount(int bcCount) {
        this.bcCount = bcCount;
    }

    @Override
    public String toString() {
        return "ProcessorInfo{" +
                "name='" + name + '\'' +
                ", netIN=" + netIN +
                ", netOut=" + netOut +
                ", reactorCount=" + reactorCount +
                ", rQueue=" + rQueue +
                ", wQueue=" + wQueue +
                ", freeBuffer=" + freeBuffer +
                ", totalBuffer=" + totalBuffer +
                ", bufferPercent=" + bufferPercent +
                ", bufferWarns=" + bufferWarns +
                ", fcCount=" + fcCount +
                ", bcCount=" + bcCount +
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

            String sql = "select name from t_processor where name = '" + getName() + "'";

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
            sql = "INSERT INTO t_processor VALUES('"
                    + getName() + "',"
                    + getNetIN() + ","
                    + getNetOut() + ","
                    + getReactorCount() + ","
                    + getrQueue() + ","
                    + getwQueue() + ","
                    + getFreeBuffer() +","
                    + getTotalBuffer() + ","
                    + getBufferPercent() + ","
                    + getBufferWarns() + ","
                    + getFcCount() + ","
                    + getBcCount() +")";
        }else {
            sql = "UPDATE t_processor SET net_in= " + getNetIN() + ","
                    + "net_out = " + getNetOut() + ","
                    + "reactor_count = " + getReactorCount() + ","
                    + "r_queue = " + getrQueue() + ","
                    + "w_queue = " + getwQueue() + ","
                    + "free_buffer = " + getFreeBuffer() + ","
                    + "total_buffer = " + getTotalBuffer() + ","
                    + "bu_percent = " + getBufferPercent() + ","
                    + "bu_warns = " + getBufferWarns() + ","
                    + "fc_count = " + getFcCount() + ","
                    + "bc_count = " + getBcCount() +
                    " WHERE name = '" + getName() + "'";
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
