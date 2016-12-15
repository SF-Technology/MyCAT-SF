package org.opencloudb.monitor;

/**
 * Mycat memory type
 *
 * @author zagnix
 * @version 1.0
 * @create 2016-11-01 10:31
 */
public enum MemoryType {
    ONHEAP("on-heap",0),NetMemory("net-memory",1),MergeMemory("merge-memory",2);

    private String name;
    private int index;

    MemoryType(String name, int index) {
        this.name = name;
        this.index = index;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    @Override
    public String toString() {
        return "MemoryType{" +
                "name='" + name + '\'' +
                ", index=" + index +
                '}';
    }
}
