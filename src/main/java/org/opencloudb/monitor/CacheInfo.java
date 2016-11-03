package org.opencloudb.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author zagnix
 * @create 2016-11-03 16:14
 */

public class CacheInfo {

    private final static Logger LOGGER =
            LoggerFactory.getLogger(CacheInfo.class);

    private String cache;
    private long max;
    private long cur;
    private long access;
    private long hit;
    private long put;
    private long lastAccess;
    private long lastPut;

    public String getCache() {
        return cache;
    }

    public void setCache(String cache) {
        this.cache = cache;
    }

    public long getMax() {
        return max;
    }

    public void setMax(long max) {
        this.max = max;
    }

    public long getCur() {
        return cur;
    }

    public void setCur(long cur) {
        this.cur = cur;
    }

    public long getAccess() {
        return access;
    }

    public void setAccess(long access) {
        this.access = access;
    }

    public long getHit() {
        return hit;
    }

    public void setHit(long hit) {
        this.hit = hit;
    }

    public long getPut() {
        return put;
    }

    public void setPut(long put) {
        this.put = put;
    }

    public long getLastAccess() {
        return lastAccess;
    }

    public void setLastAccess(long lastAccess) {
        this.lastAccess = lastAccess;
    }

    public long getLastPut() {
        return lastPut;
    }

    public void setLastPut(long lastPut) {
        this.lastPut = lastPut;
    }


    @Override
    public String toString() {
        return "CacheInfo{" +
                "cache='" + cache + '\'' +
                ", max=" + max +
                ", cur=" + cur +
                ", access=" + access +
                ", hit=" + hit +
                ", put=" + put +
                ", lastAccess=" + lastAccess +
                ", lastPut=" + lastPut +
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

            String sql = "select cache from t_cache where cache = '" + getCache() + "'";

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
            sql = "INSERT INTO t_cache VALUES('"
                    + getCache() + "',"
                    + getMax() + ","
                    + getCur() + ","
                    + getAccess() + ","
                    + getHit() + ","
                    + getPut() + ","
                    + getLastAccess() + ","
                    + getLastPut() +")";
        }else {

            sql = "UPDATE t_cache SET  max=" + getMax() +","
                    + "cur = " + getCur() + ","
                    + "access = " + getAccess() + ","
                    + "hit = " + getHit() + ","
                    + "put =" + getPut() + ","
                    + "last_access = " + getLastAccess() + ","
                    + "last_put = " + getLastPut() +
                    " WHERE cache = '" + getCache() + "'";
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
