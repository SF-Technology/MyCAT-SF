package org.opencloudb.monitor;

/**
 * SQLRecord 属性的子集
 *
 * @author zagnix
 * @create 2016-11-10 18:01
 */

public class SQLRecordSub {

    private String originalSql;
    private String user;
    private String host;
    private String tables;
    private String schema;
    private int sqlType;
    private long resultRows;
    private long exeTimes;
    private long sqlexecTime;


    public String getTables() {
        return tables;
    }

    public void setTables(String tables) {
        this.tables = tables;
    }

    public String getOriginalSql() {
        return originalSql;
    }

    public void setOriginalSql(String originalSql) {
        this.originalSql = originalSql;
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

    public long getResultRows() {
        return resultRows;
    }

    public void setResultRows(long resultRows) {
        this.resultRows = resultRows;
    }

    public long getExeTimes() {
        return exeTimes;
    }

    public void setExeTimes(long exeTimes) {
        this.exeTimes = exeTimes;
    }

    public long getSqlexecTime() {
        return sqlexecTime;
    }

    public void setSqlexecTime(long sqlexecTime) {
        this.sqlexecTime = sqlexecTime;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }
    public int getSqlType() {
        return sqlType;
    }

    public void setSqlType(int sqlType) {
        this.sqlType = sqlType;
    }

    @Override
    public String toString() {
        return "SQLRecordSub{" +
                "originalSql='" + originalSql + '\'' +
                ", user='" + user + '\'' +
                ", host='" + host + '\'' +
                ", tables='" + tables + '\'' +
                ", schema='" + schema + '\'' +
                ", sqlType=" + sqlType +
                ", resultRows=" + resultRows +
                ", exeTimes=" + exeTimes +
                ", sqlexecTime=" + sqlexecTime +
                '}';
    }
}
