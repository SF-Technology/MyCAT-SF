package org.opencloudb.sqlfw;

/**
 * @author zagnix
 * @create 2016-10-20 17:49
 */

public interface H2DBInterface<V>{
     void update();
     void insert();
     V query(String key);
     void delete();
}
