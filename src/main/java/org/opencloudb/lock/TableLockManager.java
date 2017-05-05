package org.opencloudb.lock;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
	private Map<String, GlobalTableLockHolder> gTableLockHolders;
	// 非全局表锁持有者集合
	private Map<String, TableLockHolder> tableLockHolders;
	
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
	
	public TableLockHolder getTableLockHolder(String schemaName, String tableName) {
		return this.tableLockHolders.get(getKey(schemaName, tableName));
	}
	
	public GlobalTableLockHolder getGlobalTableLockHolder(String schemaName, String tableName) {
		return this.gTableLockHolders.get(getKey(schemaName, tableName));
	}
	
	private String getKey(String schemaName, String tableName) {
		return schemaName + "." + tableName;
	}
	
}
