package org.opencloudb.monitor;

import java.sql.Types;

/**
 * @author zagnix
 * @create 2016-11-01 16:59
 */

public class ColMetaData {

    private String colName;
    private int colType;

    public ColMetaData(String colName,int colType){
        this.colName = colName;
        this.colType = colType;
    }

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    public int getColType() {
        return colType;
    }

    public void setColType(int colType) {
        this.colType = colType;
    }

    @Override
    public String toString() {
        return "ColMetaData{" +
                "colName='" + colName + '\'' +
                ", colType=" + colType +
                '}';
    }
}
