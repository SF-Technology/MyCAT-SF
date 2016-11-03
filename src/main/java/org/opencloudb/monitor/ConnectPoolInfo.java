package org.opencloudb.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author zagnix
 * @create 2016-11-02 16:50
 */

public class ConnectPoolInfo {
    private final static Logger LOGGER =
            LoggerFactory.getLogger(ConnectPoolInfo.class);
    private String processorName;
    private long id;
    private long mysqlId;
    private String host;
    private int port;
    private int l_port;
    private long net_in;
    private long net_out;
    private long life;
    private String closed;
    private String borrowed;
    private int send_queue;
    private String schema;
    private String charset;
    private String txlevel;
    private String autocommit;


    public String getProcessorName() {
        return processorName;
    }

    public void setProcessorName(String processorName) {
        this.processorName = processorName;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getMysqlId() {
        return mysqlId;
    }

    public void setMysqlId(long mysqlId) {
        this.mysqlId = mysqlId;
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

    public int getL_port() {
        return l_port;
    }

    public void setL_port(int l_port) {
        this.l_port = l_port;
    }

    public long getNet_in() {
        return net_in;
    }

    public void setNet_in(long net_in) {
        this.net_in = net_in;
    }

    public long getNet_out() {
        return net_out;
    }

    public void setNet_out(long net_out) {
        this.net_out = net_out;
    }

    public long getLife() {
        return life;
    }

    public void setLife(long life) {
        this.life = life;
    }

    public String getClosed() {
        return closed;
    }

    public void setClosed(String closed) {
        this.closed = closed;
    }

    public String getBorrowed() {
        return borrowed;
    }

    public void setBorrowed(String borrowed) {
        this.borrowed = borrowed;
    }

    public int getSend_queue() {
        return send_queue;
    }

    public void setSend_queue(int send_queue) {
        this.send_queue = send_queue;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public String getTxlevel() {
        return txlevel;
    }

    public void setTxlevel(String txlevel) {
        this.txlevel = txlevel;
    }

    public String getAutocommit() {
        return autocommit;
    }

    public void setAutocommit(String autocommit) {
        this.autocommit = autocommit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConnectPoolInfo that = (ConnectPoolInfo) o;

        if (id != that.id) return false;
        if (mysqlId != that.mysqlId) return false;
        if (host != that.host) return false;
        if (port != that.port) return false;
        if (l_port != that.l_port) return false;
        if (net_in != that.net_in) return false;
        if (net_out != that.net_out) return false;
        if (life != that.life) return false;
        if (send_queue != that.send_queue) return false;
        if (processorName != null ? !processorName.equals(that.processorName) : that.processorName != null)
            return false;
        if (closed != null ? !closed.equals(that.closed) : that.closed != null) return false;
        if (borrowed != null ? !borrowed.equals(that.borrowed) : that.borrowed != null) return false;
        if (schema != null ? !schema.equals(that.schema) : that.schema != null) return false;
        if (charset != null ? !charset.equals(that.charset) : that.charset != null) return false;
        if (txlevel != null ? !txlevel.equals(that.txlevel) : that.txlevel != null) return false;
        return autocommit != null ? autocommit.equals(that.autocommit) : that.autocommit == null;

    }


    @Override
    public String toString() {
        return "ConnectPoolInfo{" +
                "processorName='" + processorName + '\'' +
                ", id=" + id +
                ", mysqlId=" + mysqlId +
                ", host=" + host +
                ", port=" + port +
                ", l_port=" + l_port +
                ", net_in=" + net_in +
                ", net_out=" + net_out +
                ", life=" + life +
                ", closed='" + closed + '\'' +
                ", borrowed='" + borrowed + '\'' +
                ", send_queue=" + send_queue +
                ", schema='" + schema + '\'' +
                ", charset='" + charset + '\'' +
                ", txlevel='" + txlevel + '\'' +
                ", autocommit='" + autocommit + '\'' +
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

            String sql = "select id from t_connectpool where id = " + getId();

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
            sql = "INSERT INTO t_connectpool VALUES('" + getProcessorName()+ "',"
                    + getId() + ","
                    + getMysqlId() + ",'"
                    + getHost() + "',"
                    + getPort() + ","
                    + getL_port() +","
                    + getNet_in() +","
                    + getNet_out() + ","
                    + getLife() + ",'"
                    + getClosed() + "','"
                    + getBorrowed() + "',"
                    + getSend_queue() + ",'"
                    + getSchema() + "','"
                    + getCharset() + "','"
                    + getTxlevel() + "','"
                    + getAutocommit() + "')";
        }else {
            sql = "UPDATE t_connectpool SET processor ='" + getProcessorName() +"'," +
                    "mysqlId =" + getMysqlId() + ","  +
                    "host = '" + getHost() + "',"  +
                    "port = " + getPort() + ","  +
                    "l_port = " + getL_port()+ "," +
                    "net_in = " + getNet_in() + "," +
                    "net_out = " + getNet_out() + "," +
                    "life = " + getLife() + "," +
                    "closed = '" + getClosed() + "'," +
                    "borrowed = '" + getBorrowed() + "'," +
                    "SEND_QUEUE = " + getSend_queue() + "," +
                    "schema = '" + getSchema() + "'," +
                    "charset = '" + getCharset() + "'," +
                    "txlevel = '" + getTxlevel() + "'," +
                    "autocommit = '" + getAutocommit() + "'" +
                    " WHERE id = " + getId();
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
