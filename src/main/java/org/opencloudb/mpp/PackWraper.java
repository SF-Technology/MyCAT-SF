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
    public byte[] rowData;
    public String dataNode;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PackWraper that = (PackWraper) o;

        if (!Arrays.equals(rowData, that.rowData)) return false;
        return dataNode != null ? dataNode.equals(that.dataNode) : that.dataNode == null;

    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(rowData);
        result = 31 * result + (dataNode != null ? dataNode.hashCode() : 0);
        return result;
    }
}
