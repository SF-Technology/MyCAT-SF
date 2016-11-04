package org.opencloudb.monitor;



import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.backend.PhysicalDBPool;
import org.opencloudb.backend.PhysicalDatasource;
import org.opencloudb.buffer.BufferPool;
import org.opencloudb.cache.CachePool;
import org.opencloudb.cache.CacheService;
import org.opencloudb.cache.CacheStatic;
import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.heartbeat.DBHeartbeat;
import org.opencloudb.jdbc.JDBCConnection;
import org.opencloudb.mysql.nio.MySQLConnection;
import org.opencloudb.net.BackendAIOConnection;
import org.opencloudb.net.FrontendConnection;
import org.opencloudb.net.NIOProcessor;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.response.ShowDataNode;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;

/**
 * 监控服务，对外提供统统一接口
 *
 * @author zagnix
 * @version 1.0
 * @create 2016-11-01 10:17
 */

public class MonitorServer {

    private final static Logger LOGGER = LoggerFactory.getLogger(MonitorServer.class);
    private final NameableExecutor updateMonitorInfoExecutor;
    private final Timer timer;
    private final long mainThreadId;
    public MonitorServer(long threadId,Timer timer,NameableExecutor executor){
        SystemConfig systemConfig = MycatServer.getInstance().getConfig().getSystem();
        this.timer = timer;
        this.updateMonitorInfoExecutor = executor;
        this.mainThreadId = threadId;
        timer.schedule(doUpateMonitorInfo(),0L,systemConfig.getMonitorUpdatePeriod());
        updataDBInfo();
        updateSystemParam();
    }

    private TimerTask doUpateMonitorInfo() {
        return new TimerTask() {
            @Override
            public void run() {
                updateMonitorInfoExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        updateMemoryInfo();
                        updateHeartBeat();
                        updateDataNode();
                        updateDataSource();
                        updateCacheInfo();
                        updateProcessor();
                    }
                });
            }
        };
    }

    /**
     * 更新DB信息
     */
    private void updataDBInfo(){
        Map<String, SchemaConfig> schemas = MycatServer.getInstance().getConfig().getSchemas();
        for (String name : new TreeSet<String>(schemas.keySet())) {
            DataBaseInfo databaseInfo = new DataBaseInfo();
            databaseInfo.setDbName(name);
            databaseInfo.update();
        }
    }

    /**
     * 更新Heartbeat信息
     */

    public void updateHeartBeat(){

        MycatConfig conf = MycatServer.getInstance().getConfig();
        // host nodes
        Map<String, PhysicalDBPool> dataHosts = conf.getDataHosts();
        for (PhysicalDBPool pool : dataHosts.values()) {
            for (PhysicalDatasource ds : pool.getAllDataSources()) {
                DBHeartbeat hb = ds.getHeartbeat();
                HeartbeatInfo heartbeatInfo =new HeartbeatInfo();
               // row.add(ds.getName().getBytes());
                heartbeatInfo.setName(ds.getName());
               // row.add(ds.getConfig().getDbType().getBytes());
                heartbeatInfo.setType(ds.getConfig().getDbType());
                if (hb != null) {
                //    row.add(ds.getConfig().getIp().getBytes());
                    heartbeatInfo.setHost(ds.getConfig().getIp());
                //    row.add(IntegerUtil.toBytes(ds.getConfig().getPort()));
                    heartbeatInfo.setPort(ds.getConfig().getPort());
                //    row.add(IntegerUtil.toBytes(hb.getStatus()));
                    heartbeatInfo.setRsCode(hb.getStatus());
                //   row.add(IntegerUtil.toBytes(hb.getErrorCount()));
                    heartbeatInfo.setRetry(hb.getErrorCount());
                //    row.add(hb.isChecking() ? "checking".getBytes() : "idle".getBytes());
                    heartbeatInfo.setStatus(hb.isChecking() ? "checking": "idle");
                //   row.add(LongUtil.toBytes(hb.getTimeout()));
                    heartbeatInfo.setTimeout(hb.getTimeout());
                //    row.add(hb.getRecorder().get().getBytes());
                    heartbeatInfo.setExecuteTime(hb.getRecorder().get());
                    String lat = hb.getLastActiveTime();
                //   row.add(lat == null ? null : lat.getBytes());
                    heartbeatInfo.setLastActiveTime(lat == null ? null : lat);
                //   row.add(hb.isStop() ? "true".getBytes() : "false".getBytes());
                    heartbeatInfo.setStop(hb.isStop() ? "true":"false");
                    heartbeatInfo.update();
                }
            }
        }
    }

    /**
     * update DataNode的信息
     */
    public void updateDataNode() {
        MycatConfig conf = MycatServer.getInstance().getConfig();
        Map<String, PhysicalDBNode> dataNodes = conf.getDataNodes();
        List<String> keys = new ArrayList<String>();
        keys.addAll(dataNodes.keySet());

        for (String key : keys) {
            PhysicalDBNode node = dataNodes.get(key);
            DataNodeInfo dataNodeInfo = new DataNodeInfo();
            //row.add(StringUtil.encode(node.getName(), charset));
            dataNodeInfo.setName(node.getName());
            //row.add(StringUtil.encode(node.getDbPool().getHostName() + '/' + node.getDatabase(), charset));
            dataNodeInfo.setDatahost(node.getDbPool().getHostName() + '/' + node.getDatabase());

            PhysicalDBPool pool = node.getDbPool();
            PhysicalDatasource ds = pool.getSource();

            if (ds != null) {
                int active = ds.getActiveCountForSchema(node.getDatabase());
                int idle = ds.getIdleCountForSchema(node.getDatabase());
                //    row.add(IntegerUtil.toBytes(pool.getActivedIndex()));
                dataNodeInfo.setIndex(pool.getActivedIndex());
                //    row.add(StringUtil.encode(ds.getConfig().getDbType(), charset));
                dataNodeInfo.setType(ds.getConfig().getDbType());
                //   row.add(IntegerUtil.toBytes(active));
                dataNodeInfo.setActive(active);
                //    row.add(IntegerUtil.toBytes(idle));
                dataNodeInfo.setIdle(idle);
                //    row.add(IntegerUtil.toBytes(ds.getSize()));
                dataNodeInfo.setSize(ds.getSize());
                // } else {
                //    row.add(null);
                //    row.add(null);
                //    row.add(null);
                //    row.add(null);
                //    row.add(null);
                // }
                //  row.add(LongUtil.toBytes(ds.getExecuteCountForSchema(node.getDatabase())));
                dataNodeInfo.setExecute(ds.getExecuteCountForSchema(node.getDatabase()));
                NumberFormat nf = DecimalFormat.getInstance();
                // row.add(StringUtil.encode(nf.format(0), charset));
                dataNodeInfo.setTotalTime(0.0);
                // row.add(StringUtil.encode(nf.format(0), charset));
                dataNodeInfo.setMaxTime(0.0);
                // row.add(LongUtil.toBytes(0));
                dataNodeInfo.setMaxSql(0);
                long recoveryTime = pool.getSource().getHeartbeatRecoveryTime() - TimeUtil.currentTimeMillis();
                // row.add(LongUtil.toBytes(recoveryTime > 0 ? recoveryTime / 1000L : -1L));
                dataNodeInfo.setRecoveryTime(recoveryTime > 0 ? recoveryTime / 1000L : -1L);
                dataNodeInfo.update();
            }
        }
    }

    /**
     * 更新 Data Source信息
     */
    private void updateDataSource(){
        MycatConfig conf = MycatServer.getInstance().getConfig();
        Map<String, List<PhysicalDatasource>> dataSources =
                    new HashMap<String, List<PhysicalDatasource>>();

        for (PhysicalDBNode dn : conf.getDataNodes().values()) {
            List<PhysicalDatasource> dslst = new LinkedList<PhysicalDatasource>();
            dslst.addAll(dn.getDbPool().getAllDataSources());
            dataSources.put(dn.getName(), dslst);
        }

        for (Map.Entry<String, List<PhysicalDatasource>> dsEntry : dataSources
                .entrySet()) {
            String dataNodeName = dsEntry.getKey();
            for (PhysicalDatasource ds : dsEntry.getValue()) {
                DataSourceInfo dataSourceInfo = new DataSourceInfo();
                //row.add(StringUtil.encode(dataNodeName, charset));
                dataSourceInfo.setDataNode(dataNodeName);
                //row.add(StringUtil.encode(ds.getName(), charset));
                dataSourceInfo.setName(ds.getName());
                //row.add(StringUtil.encode(ds.getConfig().getDbType(), charset));
                dataSourceInfo.setType(ds.getConfig().getDbType());
                //row.add(StringUtil.encode(ds.getConfig().getIp(), charset));
                dataSourceInfo.setHost(ds.getConfig().getIp());
               // row.add(IntegerUtil.toBytes(ds.getConfig().getPort()));
                dataSourceInfo.setPort(ds.getConfig().getPort());
               // row.add(StringUtil.encode(ds.isReadNode() ? "R" : "W", charset));
                dataSourceInfo.setWR(ds.isReadNode() ? "R" : "W");
                //row.add(IntegerUtil.toBytes(ds.getActiveCount()));
                dataSourceInfo.setActive(ds.getActiveCount());
                //row.add(IntegerUtil.toBytes(ds.getIdleCount()));
                dataSourceInfo.setIdle(ds.getIdleCount());
                //row.add(IntegerUtil.toBytes(ds.getSize()));
                dataSourceInfo.setSize(ds.getSize());
                //row.add(LongUtil.toBytes(ds.getExecuteCount()));
                dataSourceInfo.setExecute(ds.getExecuteCount());
                dataSourceInfo.update();
            }
        }
    }

    /**
     * 更新 Cache 信息
     */
    public  void updateCacheInfo(){
        CacheService cacheService = MycatServer.getInstance().getCacheService();
        for (Map.Entry<String, CachePool> entry : cacheService
                .getAllCachePools().entrySet()) {
            String cacheName=entry.getKey();
            CachePool cachePool = entry.getValue();
            if (cachePool instanceof LayerCachePool) {
                for (Map.Entry<String, CacheStatic> staticsEntry : ((LayerCachePool) cachePool)
                        .getAllCacheStatic().entrySet()) {
                    CacheInfo cacheInfo = new CacheInfo();
                    cacheInfo.setCache(cacheName+'.'+staticsEntry.getKey());
                    cacheInfo.setMax(staticsEntry.getValue().getMaxSize());
                    cacheInfo.setCur(staticsEntry.getValue().getItemSize());
                    cacheInfo.setAccess(staticsEntry.getValue().getAccessTimes());
                    cacheInfo.setHit(staticsEntry.getValue().getHitTimes());
                    cacheInfo.setPut(staticsEntry.getValue().getPutTimes());
                    cacheInfo.setLastAccess(staticsEntry.getValue().getLastAccesTime());
                    cacheInfo.setLastPut(staticsEntry.getValue().getLastPutTime());
                    cacheInfo.update();

                }
            } else {
                CacheInfo cacheInfo = new CacheInfo();
                cacheInfo.setCache(cacheName);
                cacheInfo.setMax(cachePool.getCacheStatic().getMaxSize());
                cacheInfo.setCur(cachePool.getCacheStatic().getItemSize());
                cacheInfo.setAccess(cachePool.getCacheStatic().getAccessTimes());
                cacheInfo.setHit(cachePool.getCacheStatic().getHitTimes());
                cacheInfo.setPut(cachePool.getCacheStatic().getPutTimes());
                cacheInfo.setLastAccess(cachePool.getCacheStatic().getLastAccesTime());
                cacheInfo.setLastPut(cachePool.getCacheStatic().getLastPutTime());
                cacheInfo.update();
            }
        }
    }

    /**
     * 更新 系统参数信息
     */


    public  void updateSystemParam(){
       int len = SystemParameter.SYS_PARAM.length;
        SystemConfig systemConfig  = MycatServer.getInstance().getConfig().getSystem();
        for (int i = 0; i <len ; i++) {
            String sysParam[] = SystemParameter.SYS_PARAM[i];
            String varName = sysParam[0];
            String desc = sysParam[1];
            Object value = null;
            SystemParameter sysParameter = new SystemParameter();
            sysParameter.setVarName(varName);
            sysParameter.setDescribe(desc);
            try {
                Method method = null;
                String upperName = varName.substring(0,1).toUpperCase()
                        + varName.substring(1);
                method = systemConfig.getClass()
                        .getMethod("get" + upperName);
                value = method.invoke(systemConfig);
                sysParameter.setVarValue(String.valueOf(value));
            } catch (Exception e) {
              LOGGER.error(e.getMessage());
            }
            sysParameter.update();
        }


        int len1 = SystemParameter.SYS_PARAM_BOOL.length;
        for (int i = 0; i <len1 ; i++) {
            String sysParamBool[] = SystemParameter.SYS_PARAM_BOOL[i];
            String varName = sysParamBool[0];
            String desc = sysParamBool[1];
            Object value = null;
            SystemParameter sysParameter = new SystemParameter();
            sysParameter.setVarName(varName);
            sysParameter.setDescribe(desc);
            try {
                Method method = null;
                String upperName = varName.substring(0,1).toUpperCase()
                        + varName.substring(1);
                method = systemConfig.getClass()
                        .getMethod("is" + upperName);
                value = method.invoke(systemConfig);
                sysParameter.setVarValue(String.valueOf(value));
            } catch (Exception e) {
                LOGGER.error(e.getMessage());
            }
            sysParameter.update();
        }

    }

    /**
     * 更新 Processor信息
     */

    public void updateProcessor(){
        for (NIOProcessor processor : MycatServer.getInstance().getProcessors()) {
            BufferPool bufferPool=processor.getBufferPool();
            long bufferSize=bufferPool.size();
            long bufferCapacity=bufferPool.capacity();
            long bufferSharedOpts=bufferPool.getSharedOptsCount();
            long bufferUsagePercent=(bufferCapacity-bufferSize)*100/bufferCapacity;

            ProcessorInfo processorInfo = new ProcessorInfo();
           // row.add(processor.getName().getBytes());
            processorInfo.setName(processor.getName());
           // row.add(LongUtil.toBytes(processor.getNetInBytes()));
            processorInfo.setNetIN(processor.getNetInBytes());
           // row.add(LongUtil.toBytes(processor.getNetOutBytes()));
            processorInfo.setNetOut(processor.getNetOutBytes());
           // row.add(LongUtil.toBytes(0));
            processorInfo.setReactorCount(0);
           // row.add(IntegerUtil.toBytes(0));
            processorInfo.setrQueue(0);
           // row.add(IntegerUtil.toBytes(processor.getWriteQueueSize()));
            processorInfo.setwQueue(processor.getWriteQueueSize());
           // row.add(LongUtil.toBytes(bufferSize));
            processorInfo.setFreeBuffer(bufferSize);
           // row.add(LongUtil.toBytes(bufferCapacity));
            processorInfo.setTotalBuffer(bufferCapacity);
           // row.add(LongUtil.toBytes(bufferUsagePercent));
            processorInfo.setBufferPercent((int) bufferUsagePercent);
           // row.add(LongUtil.toBytes(bufferSharedOpts));
            processorInfo.setBufferWarns((int)bufferSharedOpts);
           // row.add(IntegerUtil.toBytes(processor.getFrontends().size()));
            processorInfo.setFcCount(processor.getFrontends().size());
           // row.add(IntegerUtil.toBytes(processor.getBackends().size()));
            processorInfo.setBcCount(processor.getBackends().size());
            processorInfo.update();
        }
    }
    /**
     * 获取系统内存运行状态，写入H2DB库中
     */
    private void updateMemoryInfo(){
        /**
         * 更新memory info
         */
        Runtime rt = Runtime.getRuntime();
        long total = rt.totalMemory();
        long max = rt.maxMemory();
        long used = (total - rt.freeMemory());
        MemoryInfo memoryInfo = new MemoryInfo();
        memoryInfo.setThreadId(mainThreadId);
        memoryInfo.setThreadName("Main-" +mainThreadId);
        memoryInfo.setMemoryType(MemoryType.ONHEAP.getName());
        memoryInfo.setUsed(used);
        memoryInfo.setMax(max);
        memoryInfo.setTotal(total);
        memoryInfo.update();

        /**
         * 更新thread pool
         */
        List<NameableExecutor> executors = getSysExecutors();
        for (NameableExecutor exec : executors) {
            if (exec != null) {
                ThreadPoolInfo threadPoolInfo = new ThreadPoolInfo();
                threadPoolInfo.setThreadName(exec.getName());
                threadPoolInfo.setPoolSize(exec.getPoolSize());
                threadPoolInfo.setActiveCount(exec.getActiveCount());
                threadPoolInfo.setTaskQueueSize(exec.getQueue().size());
                threadPoolInfo.setCompeletedTask(exec.getCompletedTaskCount());
                threadPoolInfo.setTotalTask(exec.getTaskCount());
                threadPoolInfo.update();
            }
        }

        /**
         * 更新connect pool
         */
        for (NIOProcessor p : MycatServer.getInstance().getProcessors()) {
            for (BackendConnection bc : p.getBackends().values()) {
                if (bc != null) {
                    ConnectPoolInfo connectPoolInfo = new ConnectPoolInfo();

                    if (bc instanceof BackendAIOConnection) {
                        //row.add(((BackendAIOConnection) c).getProcessor().getName().getBytes());
                        connectPoolInfo.setProcessorName(((BackendAIOConnection)bc).getProcessor().getName());
                    } else if(bc instanceof JDBCConnection){
                        //row.add(((JDBCConnection)c).getProcessor().getName().getBytes());
                        connectPoolInfo.setProcessorName(((JDBCConnection)bc).getProcessor().getName());
                    }else{
                       // row.add("N/A".getBytes());
                        connectPoolInfo.setProcessorName("N/A");
                    }
                    connectPoolInfo.setId(bc.getId());
                    //row.add(LongUtil.toBytes(c.getId()));
                    long threadId = 0;
                    if (bc instanceof MySQLConnection) {
                        threadId = ((MySQLConnection) bc).getThreadId();

                    }

                    //row.add(LongUtil.toBytes(threadId));
                    connectPoolInfo.setMysqlId(threadId);
                    //row.add(StringUtil.encode(c.getHost(), charset));
                    connectPoolInfo.setHost(bc.getHost());
                    //row.add(IntegerUtil.toBytes(c.getPort()));
                    connectPoolInfo.setPort(bc.getPort());
                    //row.add(IntegerUtil.toBytes(c.getLocalPort()));
                    connectPoolInfo.setL_port(bc.getLocalPort());
                    //row.add(LongUtil.toBytes(c.getNetInBytes()));
                    connectPoolInfo.setNet_in(bc.getNetInBytes());
                    //row.add(LongUtil.toBytes(c.getNetOutBytes()));
                    connectPoolInfo.setNet_out(bc.getNetOutBytes());
                    //row.add(LongUtil.toBytes((TimeUtil.currentTimeMillis() - c.getStartupTime()) / 1000L));
                    connectPoolInfo.setLife((TimeUtil.currentTimeMillis() - bc.getStartupTime()) / 1000L);
                    //row.add(c.isClosed() ? "true".getBytes() : "false".getBytes());
                    connectPoolInfo.setClosed(bc.isClosed() ? "true":"false");
                    //boolean isBorrowed = c.isBorrowed();
                    //row.add(isBorrowed ? "true".getBytes() : "false".getBytes());
                    connectPoolInfo.setBorrowed(bc.isBorrowed() ? "true":"false");
                    int writeQueueSize = 0;
                    String schema = "";
                    String charsetInf = "";
                    String txLevel = "";
                    String txAutommit = "";

                    if (bc instanceof MySQLConnection) {
                        MySQLConnection mysqlC = (MySQLConnection) bc;
                        writeQueueSize = mysqlC.getWriteQueue().size();
                        schema = mysqlC.getSchema();
                        charsetInf = mysqlC.getCharset() + ":" + mysqlC.getCharsetIndex();
                        txLevel = mysqlC.getTxIsolation() + "";
                        txAutommit = mysqlC.isAutocommit() + "";
                    }

                    //row.add(IntegerUtil.toBytes(writeQueueSize));
                    connectPoolInfo.setSend_queue(writeQueueSize);
                    //row.add(schema.getBytes());
                    connectPoolInfo.setSchema(schema);
                    //row.add(charsetInf.getBytes());
                    connectPoolInfo.setCharset(charsetInf);
                    //row.add(txLevel.getBytes());
                    connectPoolInfo.setTxlevel(txLevel);
                    //row.add(txAutommit.getBytes());
                    connectPoolInfo.setAutocommit(txAutommit);
                    connectPoolInfo.update();
                }
            }
        }

        /**
         * 更新Mycat Client Connection信息
         */
        for (NIOProcessor p : MycatServer.getInstance().getProcessors()) {
            for (FrontendConnection fc : p.getFrontends().values()) {
                if (!fc.isClosed()) {
                    ClientConnectionInfo clientConnInfo = new ClientConnectionInfo();
                    //row.add(c.getProcessor().getName().getBytes());
                    clientConnInfo.setProcessor(fc.getProcessor().getName());
                    // row.add(LongUtil.toBytes(c.getId()));
                    clientConnInfo.setId(fc.getId());
                   // row.add(StringUtil.encode(c.getHost(), charset));
                    clientConnInfo.setHost(fc.getHost());
                   // row.add(IntegerUtil.toBytes(c.getPort()));
                    clientConnInfo.setPort(fc.getPort());
                   // row.add(IntegerUtil.toBytes(c.getLocalPort()));
                    clientConnInfo.setLocalPort(fc.getLocalPort());
                   // row.add(StringUtil.encode(c.getUser(), charset));
                    clientConnInfo.setUser(fc.getUser());
                   // row.add(StringUtil.encode(c.getSchema(), charset));
                    clientConnInfo.setSchema(fc.getSchema());
                   // row.add(StringUtil.encode(c.getCharset()+":"+c.getCharsetIndex(), charset));
                    clientConnInfo.setCharset(fc.getCharset()+":"+fc.getCharsetIndex());
                   // row.add(LongUtil.toBytes(c.getNetInBytes()));
                    clientConnInfo.setNetIn(fc.getNetInBytes());
                   // row.add(LongUtil.toBytes(c.getNetOutBytes()));
                    clientConnInfo.setNetOut(fc.getNetOutBytes());
                  //  row.add(LongUtil.toBytes((TimeUtil.currentTimeMillis() - c.getStartupTime()) / 1000L));
                    clientConnInfo.setAliveTime((TimeUtil.currentTimeMillis() - fc.getStartupTime()) / 1000L);
                     ByteBuffer bb = fc.getReadBuffer();
                   // row.add(IntegerUtil.toBytes(bb == null ? 0 : bb.capacity()));
                    clientConnInfo.setRecvBuffer(bb == null ? 0 : bb.capacity());
                   // row.add(IntegerUtil.toBytes(c.getWriteQueue().size()));
                    clientConnInfo.setSendQueue(fc.getWriteQueue().size());
                    String txLevel = "";
                    String txAutommit = "";
                    if (fc instanceof ServerConnection) {
                        ServerConnection mysqlC = (ServerConnection) fc;
                        txLevel = mysqlC.getTxIsolation() + "";
                        txAutommit = mysqlC.isAutocommit() + "";
                    }
                   // row.add(txLevel.getBytes());
                    clientConnInfo.setTxLevel(txLevel);
                   // row.add(txAutommit.getBytes());
                    clientConnInfo.setAutoCommit(txAutommit);
                    clientConnInfo.update();
                }
            }
        }
    }

    private static List<NameableExecutor> getSysExecutors() {
        List<NameableExecutor> list = new LinkedList<NameableExecutor>();
        MycatServer server = MycatServer.getInstance();
        list.add(server.getTimerExecutor());
        list.add(server.getUpdateMonitorInfoExecutor());
        list.add(server.getBusinessExecutor());
        return list;
    }
}
