package org.opencloudb.manager.handler;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.backend.PhysicalDBPool;
import org.opencloudb.backend.PhysicalDatasource;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.loader.xml.jaxb.DatabaseJAXB;
import org.opencloudb.config.model.DBHostConfig;
import org.opencloudb.config.model.DataHostConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.config.util.ConfigException;
import org.opencloudb.config.util.DnPropertyUtil;
import org.opencloudb.config.util.JAXBUtil;
import org.opencloudb.jdbc.JDBCDatasource;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.statement.MycatCreateDataHostStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateDataHostStatement.Host;
import org.opencloudb.mysql.nio.MySQLDataSource;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.util.DecryptUtil;

import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class CreateDataHostHandler {
	public static final Logger LOGGER = Logger.getLogger(CreateDataHostHandler.class);

	private static final int DEFAULT_SLAVE_THRESHOLD = -1;
	private static final boolean DEFAULT_TEMP_READHOST_AVAILABLE = false;
	private static final int DEFAULT_WRITE_TYPE = PhysicalDBPool.WRITE_ONLYONE_NODE;
	private static final long DEFAULT_LOG_TIME = PhysicalDBPool.LONG_TIME;

	private static final int DEFAULT_WEIGHT = PhysicalDBPool.WEIGHT;

	public static void handle(ManagerConnection c, MycatCreateDataHostStatement stmt, String sql) {
		final ReentrantLock lock = MycatServer.getInstance().getConfig().getLock();
		lock.lock();
		try {
			ListenableFuture<Boolean> listenableFuture = MycatServer.getInstance().getListeningExecutorService()
					.submit(new CreateDataHostTask(stmt));
			Futures.addCallback(listenableFuture, new CreateDataHostCallBack(c),
					MycatServer.getInstance().getListeningExecutorService());
		} finally {
			lock.unlock();
		}
	}

	private static class CreateDataHostTask implements Callable<Boolean> {
		private MycatCreateDataHostStatement stmt;

		public CreateDataHostTask(MycatCreateDataHostStatement stmt) {
			this.stmt = stmt;
		}

		@Override
		public Boolean call() throws Exception {
			return createDataHost(stmt);
		}

	}

	/**
	 * 异步执行回调类，用来返回结果到客户端。
	 */
	private static class CreateDataHostCallBack implements FutureCallback<Boolean> {

		private ManagerConnection c;

		private CreateDataHostCallBack(ManagerConnection c) {
			this.c = c;
		}

		@Override
		public void onSuccess(Boolean result) {
			if (result) {
				ByteBuffer buffer = c.allocate();
				c.write(c.writeToBuffer(OkPacket.OK, buffer));
			} else {
				c.writeErrMessage(ErrorCode.ER_YES, "Fail to create datahost");
			}
		}

		@Override
		public void onFailure(Throwable t) {
			LOGGER.error(t.getMessage(), t);
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, t.getMessage());
		}
	}

	public static boolean createDataHost(MycatCreateDataHostStatement stmt) throws Exception {
		MycatConfig mycatConf = MycatServer.getInstance().getConfig();

		Map<String, PhysicalDBNode> dataNodes = mycatConf.getDataNodes();
		Map<String, PhysicalDBPool> dataHosts = mycatConf.getDataHosts();

		String hostName = stmt.getDatahost().getSimpleName();

		if (dataHosts.containsKey(hostName)) {
			throw new Exception("DataHost " + hostName + " already exists.");
		}

		// 生成datahost对应的连接池对象
		PhysicalDBPool pool = createPool(stmt);
		pool.setSchemas(new String[] {}); // 初始化的datahost没有datanode引用，所以为空数组

		// 初始化datahost对应的连接池
		String index = DnPropertyUtil.loadDnIndexProps().getProperty(pool.getHostName(), "0");
		if (!"0".equals(index)) {
			LOGGER.info("init datahost: " + pool.getHostName() + "  to use datasource index:" + index);
		}

		pool.init(Integer.valueOf(index));
		pool.startHeartbeat();

		if (!pool.isInitSuccess()) {
			pool.stopHeartbeat();
			pool.clearDataSources("init datahost " + pool.getHostName() + " failed.");
			throw new Exception("init datahost " + pool.getHostName() + " failed.");
		}

		// 生成dataHosts信息的副本，更新副本的信息，然后将其刷入文件中
		Map<String, PhysicalDBPool> dataHostsCopy = new TreeMap<String, PhysicalDBPool>(dataHosts);
		dataHostsCopy.put(pool.getHostName(), pool);

		DatabaseJAXB databaseJAXB = JAXBUtil.toDatabaseJAXB(dataNodes, dataHostsCopy);

		if (!JAXBUtil.flushDatabase(databaseJAXB)) {
			throw new Exception("flush database.xml failed.");
		}

		// 更新内存中的dataNode信息
		dataHosts.put(pool.getHostName(), pool);

		return true;
	}

	public static PhysicalDBPool createPool(MycatCreateDataHostStatement stmt) {
		DataHostConfig dataHostConfig = acquireHostConfig(stmt);
		PhysicalDBPool pool = getPhysicalDBPool(dataHostConfig);

		return pool;
	}

	/**
	 * 根据create datahost语句生成相应的DataHostConfig配置
	 * 
	 * @param stmt
	 * @return
	 */
	public static DataHostConfig acquireHostConfig(MycatCreateDataHostStatement stmt) {
		String name = stmt.getDatahost().getSimpleName();
		int maxCon = stmt.getMaxCon().getNumber().intValue();
		int minCon = stmt.getMinCon().getNumber().intValue();
		int balance = stmt.getBalance().getNumber().intValue();
		int switchType = stmt.getSwitchType().getNumber().intValue();

		String dbDriver = ((SQLCharExpr) stmt.getDbDriver()).getText();
		String dbType = ((SQLCharExpr) stmt.getmDbType()).getText();

		/*
		 * 一些默认配置项
		 */
		int slaveThreshold = DEFAULT_SLAVE_THRESHOLD;
		boolean tempReadHostAvailable = DEFAULT_TEMP_READHOST_AVAILABLE;
		int writeType = DEFAULT_WRITE_TYPE;
		long logTime = DEFAULT_LOG_TIME;

		List<Host> writeHosts = stmt.getWriteHosts();
		DBHostConfig[] writeDbConfs = new DBHostConfig[writeHosts.size()];
		Map<Integer, DBHostConfig[]> readHostsMap = new HashMap<Integer, DBHostConfig[]>();

		for (int w = 0; w < writeDbConfs.length; w++) {
			Host writeHost = writeHosts.get(w);
			writeDbConfs[w] = acquireDBHostConfig(name, writeHost, dbType, dbDriver, maxCon, minCon, logTime);
			List<Host> readHosts = writeHost.getReadHosts();

			DBHostConfig[] readDbConfs = new DBHostConfig[readHosts.size()];
			for (int r = 0; r < readDbConfs.length; r++) {
				Host readHost = readHosts.get(r);
				readDbConfs[r] = acquireDBHostConfig(name, readHost, dbType, dbDriver, maxCon, minCon, logTime);
			}
			readHostsMap.put(w, readDbConfs);
		}

		DataHostConfig hostConf = new DataHostConfig(name, dbType, dbDriver, writeDbConfs, readHostsMap, switchType,
				slaveThreshold, tempReadHostAvailable);

		hostConf.setMaxCon(maxCon);
		hostConf.setMinCon(minCon);
		hostConf.setBalance(balance);
		hostConf.setWriteType(writeType);
		hostConf.setLogTime(logTime);

		return hostConf;
	}

	/**
	 * 获得DBHostConfig对象
	 * 
	 * @param dataHost
	 * @param host
	 * @param dbType
	 * @param dbDriver
	 * @param maxCon
	 * @param minCon
	 * @param logTime
	 * @return
	 */
	public static DBHostConfig acquireDBHostConfig(String dataHost, Host host, String dbType, String dbDriver,
			int maxCon, int minCon, long logTime) {
		String nodeHost = ((SQLCharExpr) host.getHost()).getText();
		String nodeUrl = ((SQLCharExpr) host.getUrl()).getText();
		String user = ((SQLCharExpr) host.getUser()).getText();
		String password = ((SQLCharExpr) host.getPassword()).getText();
		String usingDecrypt = "";
		String passwordEncryty = DecryptUtil.DBHostDecrypt(usingDecrypt, nodeHost, user, password);

		int weight = DEFAULT_WEIGHT;

		String ip = null;
		int port = 0;
		if (empty(nodeHost) || empty(nodeUrl) || empty(user)) {
			throw new ConfigException(
					"dataHost " + dataHost + " define error,some attributes of this element is empty: " + nodeHost);
		}
		if ("native".equalsIgnoreCase(dbDriver)) {
			int colonIndex = nodeUrl.indexOf(':');
			ip = nodeUrl.substring(0, colonIndex).trim();
			port = Integer.parseInt(nodeUrl.substring(colonIndex + 1).trim());
		} else {
			URI url;
			try {
				url = new URI(nodeUrl.substring(5));
			} catch (Exception e) {
				throw new ConfigException("invalid jdbc url " + nodeUrl + " of " + dataHost);
			}
			ip = url.getHost();
			port = url.getPort();
		}

		DBHostConfig conf = new DBHostConfig(nodeHost, ip, port, nodeUrl, user, passwordEncryty, password);
		conf.setDbType(dbType);
		conf.setMaxCon(maxCon);
		conf.setMinCon(minCon);
		conf.setLogTime(logTime);
		conf.setWeight(weight); // 新增权重
		return conf;
	}

	private static boolean empty(String dnName) {
		return dnName == null || dnName.length() == 0;
	}

	/**
	 * 与ConfigInitializer中的实现基本相同
	 * 
	 * @param conf
	 * @return
	 */
	private static PhysicalDBPool getPhysicalDBPool(DataHostConfig conf) {
		String name = conf.getName();
		String dbType = conf.getDbType();
		String dbDriver = conf.getDbDriver();
		PhysicalDatasource[] writeSources = createDataSource(conf, name, dbType, dbDriver, conf.getWriteHosts(), false);
		Map<Integer, DBHostConfig[]> readHostsMap = conf.getReadHosts();
		Map<Integer, PhysicalDatasource[]> readSourcesMap = new HashMap<Integer, PhysicalDatasource[]>(
				readHostsMap.size());
		for (Map.Entry<Integer, DBHostConfig[]> entry : readHostsMap.entrySet()) {
			PhysicalDatasource[] readSources = createDataSource(conf, name, dbType, dbDriver, entry.getValue(), true);
			readSourcesMap.put(entry.getKey(), readSources);
		}
		PhysicalDBPool pool = new PhysicalDBPool(conf.getName(), conf, writeSources, readSourcesMap, conf.getBalance(),
				conf.getWriteType());
		return pool;
	}

	/**
	 * 与ConfigInitializer中的实现基本相同
	 * 
	 * @param conf
	 * @param hostName
	 * @param dbType
	 * @param dbDriver
	 * @param nodes
	 * @param isRead
	 * @return
	 */
	private static PhysicalDatasource[] createDataSource(DataHostConfig conf, String hostName, String dbType,
			String dbDriver, DBHostConfig[] nodes, boolean isRead) {
		SystemConfig system = MycatServer.getInstance().getConfig().getSystem();

		PhysicalDatasource[] dataSources = new PhysicalDatasource[nodes.length];
		if (dbType.equals("mysql") && dbDriver.equals("native")) {
			for (int i = 0; i < nodes.length; i++) {
				nodes[i].setIdleTimeout(system.getIdleTimeout());
				MySQLDataSource ds = new MySQLDataSource(nodes[i], conf, isRead);
				dataSources[i] = ds;
			}

		} else if (dbDriver.equals("jdbc")) {
			for (int i = 0; i < nodes.length; i++) {
				nodes[i].setIdleTimeout(system.getIdleTimeout());
				JDBCDatasource ds = new JDBCDatasource(nodes[i], conf, isRead);
				dataSources[i] = ds;
			}
		} else {
			throw new ConfigException("not supported yet !" + hostName);
		}
		return dataSources;
	}

}
