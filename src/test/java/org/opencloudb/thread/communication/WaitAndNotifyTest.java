package org.opencloudb.thread.communication;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.opencloudb.config.model.TableConfig;

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
		
		final TableConfig tableConf = new TableConfig("tb_test", "id", true, false, 
				TableConfig.TYPE_GLOBAL_TABLE, "db1,dn2,dn3", null, null, false, null, false, null, null);
		
		final AtomicBoolean flag = new AtomicBoolean(false);
		
		Thread t1 = new Thread(new Runnable() {
			
			@Override
			public void run() {
				tableConf.startChecksum();
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				flag.set(true);
				// System.out.println("end checksum");
				tableConf.endChecksum();
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
					if(tableConf.isChecksuming()) {
						tableConf.waitForChecksum();
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
