/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package org.opencloudb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.AsynchronousChannelGroup;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.log4j.Logger;
import org.opencloudb.backend.PhysicalDBPool;
import org.opencloudb.backend.infoschema.MySQLInfoSchemaProcessor;
import org.opencloudb.buffer.BufferPool;
import org.opencloudb.cache.CacheService;
import org.opencloudb.classloader.DynaClassLoader;
import org.opencloudb.config.ZkConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.interceptor.SQLInterceptor;
import org.opencloudb.lock.TableLockManager;
import org.opencloudb.manager.ManagerConnectionFactory;
import org.opencloudb.memory.MyCatMemory;
import org.opencloudb.monitor.MonitorServer;
import org.opencloudb.net.AIOAcceptor;
import org.opencloudb.net.AIOConnector;
import org.opencloudb.net.NIOAcceptor;
import org.opencloudb.net.NIOConnector;
import org.opencloudb.net.NIOProcessor;
import org.opencloudb.net.NIOReactorPool;
import org.opencloudb.net.SocketAcceptor;
import org.opencloudb.net.SocketConnector;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.route.MyCATSequnceProcessor;
import org.opencloudb.route.RouteService;
import org.opencloudb.server.ServerConnectionFactory;
import org.opencloudb.sqlengine.SQLQueryResultListener;
import org.opencloudb.sqlfw.SQLFirewallServer;
import org.opencloudb.statistic.SQLRecorder;
import org.opencloudb.util.ExecutorUtil;
import org.opencloudb.util.NameableExecutor;
import org.opencloudb.util.StringUtil;
import org.opencloudb.util.TimeUtil;

/**
 * @author mycat
 */
public class MycatServer {
	public static final String NAME = "MyCat";
	private static final long LOG_WATCH_DELAY = 60000L;
	private static final long TIME_UPDATE_PERIOD = 20L;
	private static final MycatServer INSTANCE = new MycatServer();
	private static final Logger LOGGER = Logger.getLogger("MycatServer");
	private final RouteService routerService;
	private final CacheService cacheService;
	private SQLFirewallServer sqlFirewallServer;
	private MonitorServer monitorServer;
	private Properties dnIndexProperties;
	private AsynchronousChannelGroup[] asyncChannelGroups;
	private volatile int channelIndex = 0;
	private final MyCATSequnceProcessor sequnceProcessor = new MyCATSequnceProcessor();
	private final DynaClassLoader catletClassLoader;
	private final SQLInterceptor sqlInterceptor;
	private volatile int nextProcessor;
	private BufferPool bufferPool;
	private boolean aio = false;
	private final AtomicLong xaIDInc = new AtomicLong();
	
	/**
	 * Mycat 内存管理类
	 */
	private MyCatMemory myCatMemory = null;
	public static final MycatServer getInstance() {
		return INSTANCE;
	}

	private final MycatConfig config;
	private final Timer timer;
	private final SQLRecorder sqlRecorder;
	private final AtomicBoolean isOnline;
	private final long startupTime;
	private NIOProcessor[] processors;
	private SocketConnector connector;
	private NameableExecutor businessExecutor;
	private NameableExecutor timerExecutor;
	private NameableExecutor updateMonitorInfoExecutor;
	private ListeningExecutorService listeningExecutorService;
	
	private final TableLockManager tableLockManager;

	public MycatServer() {
		//载入配置文件
		this.config = new MycatConfig();
		this.timer = new Timer(NAME + "Timer", true);
		this.sqlRecorder = new SQLRecorder(config.getSystem()
				.getSqlRecordCount());
		this.isOnline = new AtomicBoolean(true);
		cacheService = new CacheService();
		routerService = new RouteService(cacheService);
		// load datanode active index from properties
		dnIndexProperties = loadDnIndexProps();
		try {
			sqlInterceptor = (SQLInterceptor) Class.forName(
					config.getSystem().getSqlInterceptor()).newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		catletClassLoader = new DynaClassLoader(SystemConfig.getHomePath()
				+ File.separator + "catlet", config.getSystem()
				.getCatletClassCheckSeconds());
		this.tableLockManager = new TableLockManager(this.config.getSchemas().values());
		this.startupTime = TimeUtil.currentTimeMillis();
	}

	public BufferPool getBufferPool() {
		return bufferPool;
	}

	public NameableExecutor getTimerExecutor() {
		return timerExecutor;
	}

	public NameableExecutor getUpdateMonitorInfoExecutor() {
		return updateMonitorInfoExecutor;
	}

	public void setUpdateMonitorInfoExecutor(NameableExecutor updateMonitorInfoExecutor) {
		this.updateMonitorInfoExecutor = updateMonitorInfoExecutor;
	}

	public DynaClassLoader getCatletClassLoader() {
		return catletClassLoader;
	}

	public MyCATSequnceProcessor getSequnceProcessor() {
		return sequnceProcessor;
	}

	public SQLInterceptor getSqlInterceptor() {
		return sqlInterceptor;
	}

	public String genXATXID() {
		long seq = this.xaIDInc.incrementAndGet();
		if (seq < 0) {
			synchronized (xaIDInc) {
				if (xaIDInc.get() < 0) {
					xaIDInc.set(0);
				}
				seq = xaIDInc.incrementAndGet();
			}
		}
		return "'Mycat." + this.getConfig().getSystem().getMycatNodeId() + "."
				+ seq+"'";
	}

	public MyCatMemory getMyCatMemory() {
		return myCatMemory;
	}
	/**
	 * get next AsynchronousChannel ,first is exclude if multi
	 * AsynchronousChannelGroups
	 * 
	 * @return
	 */
	public AsynchronousChannelGroup getNextAsyncChannelGroup() {
		if (asyncChannelGroups.length == 1) {
			return asyncChannelGroups[0];
		} else {
			int index = (++channelIndex) % asyncChannelGroups.length;
			if (index == 0) {
				++channelIndex;
				return asyncChannelGroups[1];
			} else {
				return asyncChannelGroups[index];
			}

		}
	}

	public MycatConfig getConfig() {
		return config;
	}

	public void beforeStart() {
		String home = SystemConfig.getHomePath();
		Log4jInitializer.configureAndWatch(home + "/conf/log4j.xml", LOG_WATCH_DELAY);
		
		//ZkConfig.instance().initZk();
	}

	public void startup() throws IOException {

		SystemConfig system = config.getSystem();
		int processorCount = system.getProcessors();

		// server startup
		LOGGER.info("===============================================");
		LOGGER.info(NAME + " is ready to startup ...");
		String inf = "Startup processors ...,total processors:"
				+ system.getProcessors() + ",aio thread pool size:"
				+ system.getProcessorExecutor()
				+ "    \r\n each process allocated socket buffer pool "
				+ " bytes ,buffer chunk size:"
				+ system.getProcessorBufferChunk()
				+ "  buffer pool's capacity(buferPool/bufferChunk) is:"
				+ system.getProcessorBufferPool()
				/ system.getProcessorBufferChunk();
		LOGGER.info(inf);
		LOGGER.info("sysconfig params:" + system.toString());

		// startup manager
		ManagerConnectionFactory mf = new ManagerConnectionFactory();
		ServerConnectionFactory sf = new ServerConnectionFactory();
		SocketAcceptor manager = null;
		SocketAcceptor server = null;
		aio = (system.getUsingAIO() == 1);

		// startup processors
		int threadPoolSize = system.getProcessorExecutor();
		processors = new NIOProcessor[processorCount];
		long processBuferPool = system.getProcessorBufferPool();
		int processBufferChunk = system.getProcessorBufferChunk();
		int socketBufferLocalPercent = system.getProcessorBufferLocalPercent();
		long totalNetWorkBufferSize = 0L;
		bufferPool = new BufferPool(processBuferPool, processBufferChunk,
				socketBufferLocalPercent / processorCount);
		totalNetWorkBufferSize = processBuferPool;
		/**
		 * Off Heap For Merge/Order/Group/Limit 初始化
		 */
		if(system.getUseOffHeapForMerge() == 1){
			try {
				myCatMemory = new MyCatMemory(system,totalNetWorkBufferSize);
			} catch (NoSuchFieldException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		
		
		businessExecutor = ExecutorUtil.create("BusinessExecutor",
				threadPoolSize);
		timerExecutor = ExecutorUtil.create("Timer", system.getTimerExecutor());
		updateMonitorInfoExecutor = ExecutorUtil.create("UpdateMonitorInfo",1);
		listeningExecutorService = MoreExecutors.listeningDecorator(businessExecutor);

		for (int i = 0; i < processors.length; i++) {
			processors[i] = new NIOProcessor("Processor" + i, bufferPool,
					businessExecutor);
		}

		if (aio) {
			LOGGER.info("using aio network handler ");
			asyncChannelGroups = new AsynchronousChannelGroup[processorCount];
			// startup connector
			connector = new AIOConnector();
			for (int i = 0; i < processors.length; i++) {
				asyncChannelGroups[i] = AsynchronousChannelGroup
						.withFixedThreadPool(processorCount,
								new ThreadFactory() {
									private int inx = 1;

									@Override
									public Thread newThread(Runnable r) {
										Thread th = new Thread(r);
										th.setName(BufferPool.LOCAL_BUF_THREAD_PREX
												+ "AIO" + (inx++));
										LOGGER.info("created new AIO thread "
												+ th.getName());
										return th;
									}
								});

			}
			manager = new AIOAcceptor(NAME + "Manager", system.getBindIp(),
					system.getManagerPort(), mf, this.asyncChannelGroups[0]);

			// startup server

			server = new AIOAcceptor(NAME + "Server", system.getBindIp(),
					system.getServerPort(), sf, this.asyncChannelGroups[0]);

		} else {
			LOGGER.info("using nio network handler ");
			NIOReactorPool reactorPool = new NIOReactorPool(
					BufferPool.LOCAL_BUF_THREAD_PREX + "NIOREACTOR",
					processors.length);
			connector = new NIOConnector(BufferPool.LOCAL_BUF_THREAD_PREX
					+ "NIOConnector", reactorPool);
			((NIOConnector) connector).start();

			manager = new NIOAcceptor(BufferPool.LOCAL_BUF_THREAD_PREX + NAME
					+ "Manager", system.getBindIp(), system.getManagerPort(),
					mf, reactorPool);

			server = new NIOAcceptor(BufferPool.LOCAL_BUF_THREAD_PREX + NAME
					+ "Server", system.getBindIp(), system.getServerPort(), sf,
					reactorPool);
		}
		// manager start
		manager.start();
		LOGGER.info(manager.getName() + " is started and listening on "
				+ manager.getPort());
		server.start();
		// server started
		LOGGER.info(server.getName() + " is started and listening on "
				+ server.getPort());
		LOGGER.info("===============================================");
		sqlFirewallServer = new SQLFirewallServer();
		monitorServer = new MonitorServer(Thread.currentThread().getId(),timer,updateMonitorInfoExecutor);
		// init datahost
		Map<String, PhysicalDBPool> dataHosts = config.getDataHosts();
		LOGGER.info("Initialize dataHost ...");
		for (PhysicalDBPool node : dataHosts.values()) {
			String index = dnIndexProperties.getProperty(node.getHostName(),
					"0");
			if (!"0".equals(index)) {
				LOGGER.info("init datahost: " + node.getHostName()
						+ "  to use datasource index:" + index);
			}
			node.init(Integer.valueOf(index));
			node.startHeartbeat();
		}
		long dataNodeIldeCheckPeriod = system.getDataNodeIdleCheckPeriod();
		timer.schedule(updateTime(), 0L, TIME_UPDATE_PERIOD);
		timer.schedule(processorCheck(), 0L, system.getProcessorCheckPeriod());
		timer.schedule(dataNodeConHeartBeatCheck(dataNodeIldeCheckPeriod), 0L,
				dataNodeIldeCheckPeriod);
		timer.schedule(dataNodeHeartbeat(), 0L,
				system.getDataNodeHeartbeatPeriod());
		timer.schedule(catletClassClear(), 30000);
		/**
		 * 定期获取MySQL information_schema 中表Statistics的索引信息
		 */
		//timer.schedule(dataGetInfoSchemaStatistics(),0L,system.getInfoSchemaStatisticsGetPeriod());

	}

	private TimerTask catletClassClear() {
		return new TimerTask() {
			@Override
			public void run() {
				try {
					catletClassLoader.clearUnUsedClass();
				} catch (Exception e) {
					LOGGER.warn("catletClassClear err " + e);
				}
			};
		};
	}

	private Properties loadDnIndexProps() {
		Properties prop = new Properties();
		File file = new File(SystemConfig.getHomePath(), "conf"
				+ File.separator + "dnindex.properties");
		if (!file.exists()) {
			return prop;
		}
		FileInputStream filein = null;
		try {
			filein = new FileInputStream(file);
			prop.load(filein);
		} catch (Exception e) {
			LOGGER.warn("load DataNodeIndex err:" + e);
		} finally {
			if (filein != null) {
				try {
					filein.close();
				} catch (IOException e) {
				}
			}
		}
		return prop;
	}

	/**
	 * save cur datanode index to properties file
	 * 
	 * @param dataHost
	 * @param curIndex
	 */
	public synchronized void saveDataHostIndex(String dataHost, int curIndex) {

		File file = new File(SystemConfig.getHomePath(), "conf"
				+ File.separator + "dnindex.properties");
		FileOutputStream fileOut = null;
		try {
			String oldIndex = dnIndexProperties.getProperty(dataHost);
			String newIndex = String.valueOf(curIndex);
			if (newIndex.equals(oldIndex)) {
				return;
			}
			dnIndexProperties.setProperty(dataHost, newIndex);
			LOGGER.info("save DataHost index  " + dataHost + " cur index "
					+ curIndex);

			File parent = file.getParentFile();
			if (parent != null && !parent.exists()) {
				parent.mkdirs();
			}

			fileOut = new FileOutputStream(file);
			dnIndexProperties.store(fileOut, "update");
		} catch (Exception e) {
			LOGGER.warn("saveDataNodeIndex err:", e);
		} finally {
			if (fileOut != null) {
				try {
					fileOut.close();
				} catch (IOException e) {
				}
			}
		}

	}

	public RouteService getRouterService() {
		return routerService;
	}

	public CacheService getCacheService() {
		return cacheService;
	}

	public SQLFirewallServer getSqlFirewallServer() {
		return sqlFirewallServer;
	}
	public MonitorServer getMonitorServer() {
		return monitorServer;
	}
	public NameableExecutor getBusinessExecutor() {
		return businessExecutor;
	}

	public RouteService getRouterservice() {
		return routerService;
	}

	public NIOProcessor nextProcessor() {
		int i = ++nextProcessor;
		if (i >= processors.length) {
			i = nextProcessor = 0;
		}
		return processors[i];
	}

	public NIOProcessor[] getProcessors() {
		return processors;
	}

	public SocketConnector getConnector() {
		return connector;
	}

	public SQLRecorder getSqlRecorder() {
		return sqlRecorder;
	}

	public long getStartupTime() {
		return startupTime;
	}

	public boolean isOnline() {
		return isOnline.get();
	}

	public void offline() {
		isOnline.set(false);
	}

	public void online() {
		isOnline.set(true);
	}

	// 系统时间定时更新任务
	private TimerTask updateTime() {
		return new TimerTask() {
			@Override
			public void run() {
				TimeUtil.update();
			}
		};
	}

	// 处理器定时检查任务
	private TimerTask processorCheck() {
		return new TimerTask() {
			@Override
			public void run() {
				timerExecutor.execute(new Runnable() {
					@Override
					public void run() {
						try {
							for (NIOProcessor p : processors) {
								p.checkBackendCons();
							}
						} catch (Exception e) {
							LOGGER.warn("checkBackendCons caught err:" + e);
						}

					}
				});
				timerExecutor.execute(new Runnable() {
					@Override
					public void run() {
						try {
							for (NIOProcessor p : processors) {
								p.checkFrontCons();
							}
						} catch (Exception e) {
							LOGGER.warn("checkFrontCons caught err:" + e);
						}
					}
				});
			}
		};
	}

	// 数据节点定时连接空闲超时检查任务
	private TimerTask dataNodeConHeartBeatCheck(final long heartPeriod) {
		return new TimerTask() {
			@Override
			public void run() {
				timerExecutor.execute(new Runnable() {
					@Override
					public void run() {
						Map<String, PhysicalDBPool> nodes = config
								.getDataHosts();
						for (PhysicalDBPool node : nodes.values()) {
							node.heartbeatCheck(heartPeriod);
						}
						Map<String, PhysicalDBPool> _nodes = config
								.getBackupDataHosts();
						if (_nodes != null) {
							for (PhysicalDBPool node : _nodes.values()) {
								node.heartbeatCheck(heartPeriod);
							}
						}
					}
				});
			}
		};
	}

	// 数据节点定时心跳任务
	private TimerTask dataNodeHeartbeat() {
		return new TimerTask() {
			@Override
			public void run() {
				timerExecutor.execute(new Runnable() {
					@Override
					public void run() {
						Map<String, PhysicalDBPool> nodes = config
								.getDataHosts();
						for (PhysicalDBPool node : nodes.values()) {
							node.doHeartbeat();
						}
					}
				});
			}
		};
	}
	
	
	
	/**
	 * 定期获取MySQL后端info schema statistics 信息。
	 * @return
	 */
	private TimerTask dataGetInfoSchemaStatistics() {
		return new TimerTask() {
			@Override
			public void run() {
				timerExecutor.execute(new Runnable() {
					@Override
					public void run() {

						final String charset = getConfig().getSystem().getCharset();

						final String[] MYSQL_INFO_SCHEMA_TSTATISTICS = new String[] {
								"TABLE_SCHEMA",
								"TABLE_NAME",
								"INDEX_NAME",
								"INDEX_SCHEMA",
								"COLUMN_NAME",
								"CARDINALITY"
						};

						Map<String, PhysicalDBPool> nodes = config.getDataHosts();
						MySQLInfoSchemaProcessor processor = null;

						String execSQL = "select ";

						for (String colname: MYSQL_INFO_SCHEMA_TSTATISTICS) {
							execSQL +=colname + ",";
						}

						execSQL +="CARDINALITY from STATISTICS where TABLE_SCHEMA != 'information_schema'";

						try {
							processor = new MySQLInfoSchemaProcessor("information_schema", nodes.size(),
									execSQL, MYSQL_INFO_SCHEMA_TSTATISTICS, new SQLQueryResultListener<HashMap<String,LinkedList<byte[]>>>() {

								@Override
								public void onResult(HashMap<String,LinkedList<byte[]>> mapIterator ) {

									ConcurrentHashMap<String,Map<String,String>>  tableIndexMap =
											getConfig().getTableIndexMap();

									LinkedList<byte []> linkedList = new LinkedList<byte[]>();
									for (String key:mapIterator.keySet()) {
										linkedList = mapIterator.get(key);
										if (linkedList.size() >0)
											break;
									}

									for (int i = 0; i < linkedList.size(); i++) {
										RowDataPacket row = new RowDataPacket(MYSQL_INFO_SCHEMA_TSTATISTICS.length);
										row.read(linkedList.get(i));

										String tableName = null;
										String index = null;
										String cap = null;

										if (row.fieldValues.get(1) != null) {
											tableName = StringUtil.decode(row.fieldValues.get(1),charset);
										}
										if (row.fieldValues.get(4) != null) {
											index = StringUtil.decode(row.fieldValues.get(4),charset);
										}

										if (row.fieldValues.get(5) != null) {
											cap = StringUtil.decode(row.fieldValues.get(5),charset);
										}


										if (tableIndexMap.containsKey(tableName)){
											tableIndexMap.get(tableName).put(index,cap);
										}else {
											Map<String,String> map = new HashMap<String,String>();
											map.put(index,cap);
											tableIndexMap.put(tableName,map);
										}
									}

									/**
									for (String key:tableIndexMap.keySet()){
										Map<String,String> map = tableIndexMap.get(key);
										for (String k:map.keySet()){
											LOGGER.error("table :" + key + "," + " index :" + k  + " cap " + map.get(k));
										}
									}*/

									linkedList.clear();
								}
							});
							processor.processSQL();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
			}
		};
	}
	

	public boolean isAIO() {
		return aio;
	}

	public ListeningExecutorService getListeningExecutorService() {
		return listeningExecutorService;
	}
	
	public TableLockManager getTableLockManager() {
		return this.tableLockManager;
	}
}
