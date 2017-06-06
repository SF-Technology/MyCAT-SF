package org.opencloudb.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 心跳包信息
 *
 * @author zagnix
 * @version 1, 0
 * @create 2016-11-03 11:27
 */
public class HeartbeatInfo {

    private final static Logger LOGGER =
            LoggerFactory.getLogger(HeartbeatInfo.class);

    private String name;
    private String type;
    private String host;
    private int    port;
    private int rsCode;
    private int retry;
    private String status;
    private long   timeout;
    private String executeTime;
    private String lastActiveTime;
    private String stop;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getRsCode() {
        return rsCode;
    }

    public void setRsCode(int rsCode) {
        this.rsCode = rsCode;
    }

    public int getRetry() {
        return retry;
    }

    public void setRetry(int retry) {
        this.retry = retry;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public String getExecuteTime() {
        return executeTime;
    }

    public void setExecuteTime(String executeTime) {
        this.executeTime = executeTime;
    }

    public String getLastActiveTime() {
        return lastActiveTime;
    }

    public void setLastActiveTime(String lastActiveTime) {
        this.lastActiveTime = lastActiveTime;
    }

    public String getStop() {
        return stop;
    }

    public void setStop(String stop) {
        this.stop = stop;
    }

    @Override
    public String toString() {
        return "HeartbeatInfo{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", rsCode=" + rsCode +
                ", retry=" + retry +
                ", status='" + status + '\'' +
                ", timeout=" + timeout +
                ", executeTime='" + executeTime + '\'' +
                ", lastActiveTime='" + lastActiveTime + '\'' +
                ", stop='" + stop + '\'' +
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

            String sql = "select host  from t_heartbeat where host = '" + getHost() + "'";

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
            sql = "INSERT INTO t_heartbeat VALUES('"
                    + getHost() + "','"
                    + getType() + "','"
                    + getName() + "',"
                    + getPort() + ","
                    + getRsCode() + ","
                    + getRetry() + ",'"
                    + getStatus() + "',"
                    + getTimeout() + ",'"
                    + getExecuteTime() + "','"
                    + getLastActiveTime() + "','"
                    + getStop() +"')";
        }else {
            sql = "UPDATE t_heartbeat SET type ='" + getType() +"'," +
                    "name ='" + getName() + "',"  +
                    "port = " + getPort() + ","  +
                    "rs_code = " + getRsCode() + ","  +
                    "retry = " + getRetry()+ "," +
                    "status ='" + getStatus() + "'," +
                    "timeout =" + getTimeout() + "," +
                    "execute_time ='" + getExecuteTime() + "'," +
                    "last_active_time ='" + getLastActiveTime() + "',"+
                    "stop = '" + getStop() + "'" +
                    " WHERE host = '" + getHost() + "'";
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
