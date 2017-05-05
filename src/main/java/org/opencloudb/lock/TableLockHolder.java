package org.opencloudb.lock;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 表锁持有者, 控制一些需要用到表锁的行为
 * @author CrazyPig
 * @since 2017-01-18
 *
 */
public class TableLockHolder {
	
	private String schema;
	private String table;
	
	// 表级锁
	protected Lock tableLock;
	
	public TableLockHolder(String schema, String table) {
		this.schema = schema;
		this.table = table;
		tableLock = new ReentrantLock();
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("TableLockHolder" + this.hashCode())
			.append(" hold table lock [")
			.append(this.schema + "." + this.table)
			.append("]");
		return sb.toString();
	}

}
