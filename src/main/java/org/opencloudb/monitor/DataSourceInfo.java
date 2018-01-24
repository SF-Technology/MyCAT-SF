package org.opencloudb.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author zagnix
 * @create 2016-11-03 15:38
 */

public class DataSourceInfo {

    private final static Logger LOGGER =
            LoggerFactory.getLogger(DataSourceInfo.class);

    private String dataNode;
    private String name;
    private String type;
    private String host;
    private int port;
    private String WR;
    private int active;
    private int idle;
    private int size;
    private long execute;

    public String getDataNode() {
        return dataNode;
    }

    public void setDataNode(String dataNode) {
        this.dataNode = dataNode;
    }

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

    public String getWR() {
        return WR;
    }

    public void setWR(String WR) {
        this.WR = WR;
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

            String sql = "select datanode from t_datasource where datanode = '" + getDataNode() + "'";

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
            sql = "INSERT INTO t_datasource VALUES('"
                    + getDataNode() + "','"
                    + getName() + "','"
                    + getType() + "','"
                    + getHost() + "',"
                    + getPort() + ",'"
                    + getWR() + "',"
                    + getActive() + ","
                    + getIdle() + ","
                    + getSize() + ","
                    + getExecute() +")";
        }else {

            sql = "UPDATE t_datasource SET name ='" + getName() +"',"
                    + "type = '" + getType() + "',"
                    + "host = '" + getHost() + "',"
                    + "port = " + getPort() + ","
                    + "W_R = '" + getWR() + "',"
                    + "active = " + getActive() + ","
                    + "idle = " + getIdle() + ","
                    + "size = " + getSize() + ","
                    + "execute = " + getExecute() +
                    " WHERE datanode = '" + getDataNode() + "'";
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
