package org.opencloudb.thread.communication;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.opencloudb.lock.GlobalTableLockHolder;

import junit.framework.Assert;

/**
 * 
 * @author CrazyPig
 * @since 2017-01-13
 *
 */
public class WaitAndNotifyTest {
	
	/**
	 * 测试线程通讯, wait and notify机制
	 */
	@Test
	public void testWaitAndNotify() {
		
		final GlobalTableLockHolder gTableLockHolder = new GlobalTableLockHolder("testdb", "tb_test_cs");
		
		final AtomicBoolean flag = new AtomicBoolean(false);
		
		Thread t1 = new Thread(new Runnable() {
			
			@Override
			public void run() {
				gTableLockHolder.startChecksum();
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				flag.set(true);
				// System.out.println("end checksum");
				gTableLockHolder.endChecksum();
			}
		});
		
		t1.start();
		
		try {
			Thread.sleep(50);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		Thread[] thds = new Thread[10];
		for(int i = 0; i < thds.length; i++) {
			thds[i] = new Thread(new Runnable() {
				
				@Override
				public void run() {
					if(gTableLockHolder.isChecksuming()) {
						gTableLockHolder.waitForChecksum();
						// System.out.println("finished wait for checksum");
					}
					Assert.assertEquals(true, flag.get());
				}
			});
			thds[i].start();
			// System.out.println("thd[" + i + "] start!");
		}
		
		for(int i = 0; i < thds.length; i++) {
			try {
				thds[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}

}
