package org.opencloudb.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author zagnix
 * @create 2016-11-16 10:48
 */
public class SQLTopN {

    private final static Logger LOGGER =
            LoggerFactory.getLogger(SQLTopN.class);

    private String sql;
    private String user;
    private String host;
    private String schema;
    private String tables;

    /**
     * value 可以是执行time，执行次数，执行结果rows
     */
    private long value;


    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTables() {
        return tables;
    }

    public void setTables(String tables) {
        this.tables = tables;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }


    @Override
    public String toString() {
        return "SQLTopN{" +
                "sql='" + sql + '\'' +
                ", user='" + user + '\'' +
                ", host='" + host + '\'' +
                ", schema='" + schema + '\'' +
                ", tables='" + tables + '\'' +
                ", value=" + value +
                '}';
    }

    public void update(String tableName,String colNname) {

        if (tableName == null || colNname == null)
            return;

       int isUpdate = 0; //直接插入
       long value = 0L;

        final Connection h2DBConn =
                H2DBMonitorManager.getH2DBMonitorManager().getH2DBMonitorConn();
        Statement stmt = null;
        ResultSet rset = null;

        try {
            String sql = "select " + colNname + " from "+tableName+" where sql = '" +getSql() + "'";

            stmt = h2DBConn.createStatement();
            rset = stmt.executeQuery(sql);
            if (rset.next()){
                isUpdate = 1; //不需要更新
                value = rset.getLong(1);
                if (value < getValue()){
                    isUpdate = 2; //需要更新
                }
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

        String sql = null;

        if(isUpdate == 0) {
             sql = "INSERT INTO " + tableName + " VALUES('" + getSql() + "','"
                    + getUser() + "','"
                    + getHost() + "','"
                    + getSchema() + "','"
                    + getTables() + "',"
                    + getValue() + ")";
        }else if(isUpdate == 2){
            sql = "UPDATE "+ tableName +" SET "+ colNname + "=" + getValue() +
                    " WHERE sql = '" + getSql() + "'";
        }if (isUpdate==1){
            return;
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
