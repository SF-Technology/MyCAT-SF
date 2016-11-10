package org.opencloudb.monitor;

/**
 * SQLRecord 属性的子集
 *
 * @author zagnix
 * @create 2016-11-10 18:01
 */

public class SQLRecordSub {

    private String originalSql;
    private String host;
    private String schema;
    private long resultRows;
    private long exeTimes;
    private long sqlexecTime;


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


    @Override
    public String toString() {
        return "SQLRecordSub{" +
                "originalSql='" + originalSql + '\'' +
                ", host='" + host + '\'' +
                ", schema='" + schema + '\'' +
                ", resultRows='" + resultRows + '\'' +
                ", exeTimes='" + exeTimes + '\'' +
                ", sqlexecTime='" + sqlexecTime + '\'' +
                '}';
    }
}
