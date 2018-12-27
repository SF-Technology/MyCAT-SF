package org.opencloudb.lock;

import java.util.concurrent.locks.Condition;

/**
 * 全局表锁持有者, 控制一些要用到全局表锁的行为
 * @author CrazyPig
 * @since 2017-01-18
 *
 */
public final class GlobalTableLockHolder extends TableLockHolder {
	
	// checksum table 使用的条件
	private Condition waitForChecksumCond;
	private volatile boolean isChecksuming;

	public GlobalTableLockHolder(String schema, String table) {
		super(schema, table);
		isChecksuming = false;
		waitForChecksumCond = tableLock.newCondition();
	}
	
	public boolean isChecksuming() {
		return this.isChecksuming;
	}
	
	public void startChecksum() {
		isChecksuming = true;
	}
	
	public void waitForChecksum() {
		tableLock.lock();
		try {
			while(isChecksuming) {
				waitForChecksumCond.await();
			}
		} catch(InterruptedException e) {
			e.printStackTrace();
		} finally {
			tableLock.unlock();
		}
	}
	
	public void endChecksum() {
		tableLock.lock();
		try {
			isChecksuming = false;
			waitForChecksumCond.signalAll();
		} finally {
			tableLock.unlock();
		}
	}

}
