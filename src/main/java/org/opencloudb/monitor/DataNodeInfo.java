package org.opencloudb.monitor;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author zagnix
 * @create 2016-11-03 14:24
 */

public class DataNodeInfo {
    private final static Logger LOGGER =
            LoggerFactory.getLogger(DataNodeInfo.class);

    private String name;
    private String datahost;
    private int index;
    private String type;
    private int active;
    private int idle;
    private int size;
    private long execute;
    private double totalTime;
    private double maxTime;
    private long maxSql;
    private long recoveryTime;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDatahost() {
        return datahost;
    }

    public void setDatahost(String datahost) {
        this.datahost = datahost;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getActive() {
        return active;
    }

    public void setActive(int active) {
        this.active = active;
    }

    public int getIdle() {
        return idle;
    }

    public void setIdle(int idle) {
        this.idle = idle;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public long getExecute() {
        return execute;
    }

    public void setExecute(long execute) {
        this.execute = execute;
    }


    public double getTotalTime() {
        return totalTime;
    }

    public void setTotalTime(double totalTime) {
        this.totalTime = totalTime;
    }

    public double getMaxTime() {
        return maxTime;
    }

    public void setMaxTime(double maxTime) {
        this.maxTime = maxTime;
    }

    public long getMaxSql() {
        return maxSql;
    }

    public void setMaxSql(long maxSql) {
        this.maxSql = maxSql;
    }

    public long getRecoveryTime() {
        return recoveryTime;
    }

    public void setRecoveryTime(long recoveryTime) {
        this.recoveryTime = recoveryTime;
    }


    @Override
    public String toString() {
        return "DataNodeInfo{" +
                "name='" + name + '\'' +
                ", datahost='" + datahost + '\'' +
                ", index=" + index +
                ", type='" + type + '\'' +
                ", active=" + active +
                ", idle=" + idle +
                ", size=" + size +
                ", execute=" + execute +
                ", totalTime=" + totalTime +
                ", maxTime=" + maxTime +
                ", maxSql=" + maxSql +
                ", recoveryTime=" + recoveryTime +
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

            String sql = "select name  from t_datanode where name = '" + getName() + "'";

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
            sql = "INSERT INTO t_datanode VALUES('"
                    + getName() + "','"
                    + getDatahost() + "',"
                    + getIndex() + ",'"
                    + getType() + "',"
                    + getActive() + ","
                    + getIdle() + ","
                    + getSize() + ","
                    + getExecute() + ","
                    + getTotalTime() + ","
                    + getMaxTime() + ","
                    + getMaxSql() + ","
                    + getRecoveryTime() +")";
        }else {
            sql = "UPDATE t_datanode SET datahost ='" + getDatahost() +"',"
                    + "index = " + getIndex() + ","
                    + "type = '" + getType() + "',"
                    + "active = " + getActive() + ","
                    + "idle = " + getIdle() + ","
                    + "size = " + getIdle() + ","
                    + "execute = " + getExecute() + ","
                    + "total_time = " + getTotalTime() + ","
                    + "max_time = " + getMaxTime() + ","
                    + "max_sql = " + getMaxSql() + ","
                    + "recovery_time =" + getRecoveryTime() +
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
