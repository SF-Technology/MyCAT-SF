package org.opencloudb.lock;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.TableConfig;

/**
 * 表锁管理器
 * @author CrazyPig
 * @since 2017-01-18
 *
 */
public class TableLockManager {
	
	// 全局表锁持有者集合
	private ConcurrentMap<String, GlobalTableLockHolder> gTableLockHolders;
	// 非全局表锁持有者集合
	private ConcurrentMap<String, TableLockHolder> tableLockHolders;
	
	public TableLockManager(Iterable<SchemaConfig> schemas) {
		if(schemas == null) {
			throw new IllegalArgumentException();
		}
		tableLockHolders = new ConcurrentHashMap<String, TableLockHolder>();
		gTableLockHolders = new ConcurrentHashMap<String, GlobalTableLockHolder>();
		for(SchemaConfig schema : schemas) {
			for(TableConfig tableConf : schema.getTables().values()) {
				if(tableConf.isGlobalTable()) {
					gTableLockHolders.put(getKey(schema.getName(), tableConf.getName()), new GlobalTableLockHolder(schema.getName(), tableConf.getName()));
				} else {
					tableLockHolders.put(getKey(schema.getName(), tableConf.getName()), new TableLockHolder(schema.getName(), tableConf.getName()));
				}
			}
		}
	}
	
	public void addGlobalTableLock(String schemaName, String tableName) {
	    this.gTableLockHolders.put(getKey(schemaName, tableName), new GlobalTableLockHolder(schemaName, tableName));
	}
	
	public TableLockHolder getTableLockHolder(String schemaName, String tableName) {
	    // 获取非全局表对应的锁, 如果不存在, 则尝试创建
	    TableLockHolder lh = this.tableLockHolders.get(getKey(schemaName, tableName));
	    if (lh == null) {
	        lh = new TableLockHolder(schemaName, tableName);
	        TableLockHolder expectLh = this.tableLockHolders.putIfAbsent(getKey(schemaName, tableName), lh);
	        if (expectLh != null) {
	            lh = expectLh;
	        }
	    }
		return lh;
	}
	
	public GlobalTableLockHolder getGlobalTableLockHolder(String schemaName, String tableName) {
	    // 获取全局表对应的锁, 如果不存在, 则尝试创建
	    GlobalTableLockHolder glh = this.gTableLockHolders.get(getKey(schemaName, tableName));
	    if (glh == null) {
	        glh = new GlobalTableLockHolder(schemaName, tableName);
	        GlobalTableLockHolder expectGlh = this.gTableLockHolders.putIfAbsent(getKey(schemaName, tableName), glh);
	        if (expectGlh != null) {
	            glh = expectGlh;
	        }
	    }
	    return glh;
	}
	
	private String getKey(String schemaName, String tableName) {
		return schemaName + "." + tableName;
	}
	
}
