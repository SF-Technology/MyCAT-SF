package org.opencloudb.mpp;


/**
 * Created by zagnix on 2016/7/6.
 */

import java.util.Arrays;

/**
 * 一行数据是从哪个节点来的。
 * 通过dataNode查找对应的sorter，
 * 将数据放到对应的datanode的sorter，
 * 进行排序.
 */
public final  class PackWraper {


    public boolean failed = true;
    public byte[] rowData;
    public String dataNode;

    public boolean isFailed() {
        return failed;
    }

    public void setFailed(boolean failed) {
        this.failed = failed;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PackWraper)) return false;

        PackWraper that = (PackWraper) o;

        if (failed != that.failed) return false;
        if (!Arrays.equals(rowData, that.rowData)) return false;
        return dataNode != null ? dataNode.equals(that.dataNode) : that.dataNode == null;

    }

    @Override
    public int hashCode() {
        int result = (failed ? 1 : 0);
        result = 31 * result + Arrays.hashCode(rowData);
        result = 31 * result + (dataNode != null ? dataNode.hashCode() : 0);
        return result;
    }
}
