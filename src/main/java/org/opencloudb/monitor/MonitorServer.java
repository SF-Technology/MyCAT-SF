package org.opencloudb.monitor;



import com.alibaba.druid.sql.ast.SQLExpr;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import org.opencloudb.config.loader.zookeeper.entitiy.Server;
import org.opencloudb.config.model.FirewallConfig;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.heartbeat.DBHeartbeat;
import org.opencloudb.jdbc.JDBCConnection;
import org.opencloudb.manager.handler.CheckTableStructureConsistencyHandler;
import org.opencloudb.memory.MyCatMemory;
import org.opencloudb.memory.unsafe.Platform;
import org.opencloudb.mysql.nio.MySQLConnection;
import org.opencloudb.net.BackendAIOConnection;
import org.opencloudb.net.FrontendConnection;
import org.opencloudb.net.NIOProcessor;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.sqlfw.H2DBManager;
import org.opencloudb.sqlfw.SQLFirewallServer;
import org.opencloudb.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.*;

import static org.opencloudb.sqlfw.SQLFirewallServer.OP_UPATE;

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
    long statTime = 0L;
    //表结构一致性检测开计时
    private long startTscTime = 0;

    /**
     * 定时线程1 定期移除内存DB中sql统计信息
     * 从访问时间过期时间
     * sqlInMemDBPeriod
     * sql从第一次执行开始，在H2DB中停留sqlInMemDBPeriod，将被移除H2DB中。
     */
    private static final ThreadFactory delSqlRecordFactory =
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("async-del-sqlrecord").build();
    private static final ScheduledExecutorService delSqlRecordExecutor =
            new ScheduledThreadPoolExecutor(1,delSqlRecordFactory);

    /**
     *定时线程2 内存中的数据合并
     * select，insert，update，delete语句
     * 根据对应的表 分别统计执行次数
     * bySqlTypeSummaryPeriod
     * 从H2DB中统计四种sql语句执行的次数（精确到表级别）
     */
    private static final ThreadFactory sqlTypeSummaryFactory =
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("async-sqltype-summary").build();
    private static final ScheduledExecutorService sqlTypeSummaryExecutor =
            new ScheduledThreadPoolExecutor(1,sqlTypeSummaryFactory);

    /**
     * 定时线程3，维护TOPN执行结果集，维护TOPN，SQL执行时间
     * topNSummaryPeriod : 定期获取结果集TOPN和SQL执行时间TOPN
     * topExecuteResultN
     * topSqlExecuteTimeN
     */
    private static final ThreadFactory topNSummaryPeriodFactory =
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("async-topn-summary").build();

    private static final ScheduledExecutorService topNSummaryPeriodExecutor =
            new ScheduledThreadPoolExecutor(1,topNSummaryPeriodFactory);

    private static Comparator<SQLTopN> comparator = new Comparator<SQLTopN>() {
        @Override
        public int compare(SQLTopN left, SQLTopN right) {
            long  b = left.getValue();
            long  a = right.getValue();
            return (a < b) ? -1 : (a > b) ? 1 : 0;
        }
    };
    private final SystemConfig systemConfig;

    public MonitorServer(long threadId,Timer timer,NameableExecutor executor){
        systemConfig = MycatServer.getInstance().getConfig().getSystem();
        this.timer = timer;
        this.updateMonitorInfoExecutor = executor;
        this.mainThreadId = threadId;
        timer.schedule(doUpateMonitorInfo(),0L,systemConfig.getMonitorUpdatePeriod());

        /**TODO 改成定时更新*/
        updataDBInfo();
        updateSystemParam();

        /**
         * 定期清理过期的sql record.
         */
        statTime = System.currentTimeMillis();

        if (systemConfig.getEnableSqlStat() == 1) {
            delSqlRecordExecutor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    long expiredTime = System.currentTimeMillis() - systemConfig.getSqlInMemDBPeriod();
                    dumpToDiskDb(expiredTime);
                    deleteExpiredSqlStat(expiredTime, "t_sqlstat");
                    if (System.currentTimeMillis() - statTime >= 2 * 3600000/**60*60*1000*/) {
                        statTime = System.currentTimeMillis();
                        /**
                         * 每隔getSqlRecordInDiskPeriod天从磁盘删除过期的sql
                         */
                        long expiredDelHistoryTime = System.currentTimeMillis() -
                                systemConfig.getSqlRecordInDiskPeriod() * SystemConfig.DEFAULT_DAY_MILLISECONDS;
                        deleteExpiredSqlStat(expiredDelHistoryTime, H2DBManager.getSqlRecordTableName());
                    }

                }
            }, 0, systemConfig.getSqlInMemDBPeriod() / 2, TimeUnit.MILLISECONDS);

            /**
             * 根据sql类型，做统计分析
             */
            sqlTypeSummaryExecutor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    String sql = "select original_sql,user,host," +
                            "schema,tables,sqltype" +
                            ",result_rows,exe_times,sqlexec_time from t_sqlstat";
                    ArrayList<SQLRecordSub> arrayList = bySqlTypeSummaryPeriod(sql);
                    for (int i = 0; i < arrayList.size(); i++) {
                        SQLRecordSub sqlRecordSub = arrayList.get(i);
                        SQLTypeSummary sqlTypeSummary = new SQLTypeSummary();
                        int type = sqlRecordSub.getSqlType();
                        String user = sqlRecordSub.getUser();
                        String host = sqlRecordSub.getHost();
                        String schema = sqlRecordSub.getSchema();
                        String tables = sqlRecordSub.getTables();
                        String sqlType = null;
                        switch (type) {
                            case ServerParse.DELETE:
                                sqlType = "delete";
                                break;
                            case ServerParse.INSERT:
                                sqlType = "insert";
                                break;
                            case ServerParse.SELECT:
                                sqlType = "select";
                                break;
                            case ServerParse.UPDATE:
                                sqlType = "update";
                                break;
                            default:
                                sqlType = "";
                                break;
                        }
                        String pkey = sqlType + "-" + user + "-" + host
                                + "-" + schema + "-" + tables;
                        sqlTypeSummary.setPkey(pkey);
                        sqlTypeSummary.setSqlType(sqlType);
                        sqlTypeSummary.setUser(user);
                        sqlTypeSummary.setHost(host);
                        sqlTypeSummary.setSchema(schema);
                        sqlTypeSummary.setTables(tables);
                        sqlTypeSummary.setExecSqlCount(sqlRecordSub.getExeTimes());
                        sqlTypeSummary.setExecSqlTime(sqlRecordSub.getSqlexecTime());
                        sqlTypeSummary.setExecSqlRows(sqlRecordSub.getResultRows());
                        sqlTypeSummary.update();
                    }
                }
            }, 0, systemConfig.getSqlInMemDBPeriod() / 4, TimeUnit.MILLISECONDS);


            topNSummaryPeriodExecutor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {

                    /**
                     * 从t_sqlstat中获取topN rows
                     */
                    long topNRows = systemConfig.getTopExecuteResultN();
                    String newSql = "select original_sql,user,host,schema," +
                            "tables,result_rows" +
                            " from t_sqlstat order by result_rows limit " + topNRows;
                    String oldSql = "select sql,user,host,schema,tables,exec_rows from t_topnrows";
                    takeTopN(newSql, oldSql, "t_topnrows", "exec_rows", topNRows);


                    /**
                     * 从t_sqlstat中获取topN time
                     */
                    long topNTime = systemConfig.getTopSqlExecuteTimeN();
                    newSql = "select original_sql,user,host," +
                            "schema,tables,exe_times," +
                            "sqlexec_time from t_sqlstat order by sqlexec_time limit " + topNTime;
                    oldSql = "select sql,user,host,schema,tables,exec_time from t_topntime";
                    takeTopN(newSql, oldSql, "t_topntime", "exec_time", topNRows);


                    /**
                     *  从t_sqlstat中获取topN count
                     */
                    long topNCount = systemConfig.getTopSqlExecuteCountN();
                    newSql = "select original_sql,user,host," +
                            "schema,tables,exe_times " +
                            "from t_sqlstat order by exe_times limit " + topNCount;
                    oldSql = "select sql,user,host,schema,tables,exec_count from t_topncount";
                    takeTopN(newSql, oldSql, "t_topncount", "exec_count", topNRows);
                }
            }, 0, systemConfig.getSqlInMemDBPeriod() / 8, TimeUnit.MILLISECONDS);
        }

    }

    private TimerTask doUpateMonitorInfo() {
        startTscTime = System.currentTimeMillis();
        return new TimerTask() {
            @Override
            public void run() {
                updateMonitorInfoExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        updateMemoryInfo();
                        updateNetConnection();
                        updateHeartBeat();
                        updateDataNode();
                        updateDataSource();
                        updateCacheInfo();
                        updateProcessor();
                        if(System.currentTimeMillis() - startTscTime >= systemConfig.getCheckTSCPeriod()*60*1000) {
                            startTscTime = System.currentTimeMillis();
                            CheckTableStructureConsistency();
                        }
                    }
                });
            }
        };
    }

    /**
     * 获取最新的top n
     */
    private void  takeTopN(String newSql,String oldSql,String tableName,
                               String colName,long topN){
        PriorityQueue<SQLTopN> priorityQueue = null;

        ArrayList<SQLTopN> newTopNList = getSQLTOPN(newSql);
        ArrayList<SQLTopN> oldTopNList = getSQLTOPN(oldSql);
        priorityQueue = new PriorityQueue<SQLTopN>((int)(2*topN),comparator);

        if(oldTopNList.size() > 0 && newTopNList.size()> 0) {
            deleteTopN(tableName);
        }

        for (int i = 0; i < newTopNList.size() ; i++) {
            priorityQueue.add(newTopNList.get(i));
        }

        for (int i = 0; newTopNList.size()>0 && i < oldTopNList.size() ; i++) {
            priorityQueue.add(oldTopNList.get(i));
        }

        int queueSize = priorityQueue.size();

        if (newTopNList.size()>0 && queueSize > 0) {
            updateTopN(priorityQueue,tableName,colName, (int) topN,queueSize);
        }
    }

    /**
     * 删除t_topnxxx表中的数据
     * @param tableName
     */
    public void  deleteTopN(String tableName){

        if (tableName == null)
            return;
       final Connection h2DBConn =
                H2DBMonitorManager.getH2DBMonitorManager().getH2DBMonitorConn();
        Statement stmt = null;
        ResultSet rset = null;

        try {
            String sql = "delete from "+ tableName ;
            stmt = h2DBConn.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
        } finally {
            try {

                if (stmt != null) {
                    stmt.close();
                }

                if (rset != null) {
                    rset.close();
                }
            } catch (SQLException e) {
                LOGGER.error(e.getMessage());
            }
        }
    }

    /**
     * 更新历史TopN
     */
    public void updateTopN(PriorityQueue<SQLTopN> priorityQueue,String tableName,String colName,
                           int n,int queueSize){
            int count = Math.min(n,queueSize);
            while (count > 0){
                SQLTopN sqlTopN = priorityQueue.remove();
                sqlTopN.update(tableName,colName);
                count--;
            }

            if(queueSize>0)
                priorityQueue.clear();
    }

    /**
     * 获取历史TOPN
     * @param sql
     * @return
     */
    private ArrayList<SQLTopN> getSQLTOPN(String sql){
        ArrayList<SQLTopN> sqlTopNs = new ArrayList<SQLTopN>();
        final Connection h2DBConn =
                H2DBMonitorManager.getH2DBMonitorManager().getH2DBMonitorConn();
        Statement stmt = null;
        ResultSet rset = null;
        try {
            stmt = h2DBConn.createStatement();
            rset = stmt.executeQuery(sql);
            while (rset.next()){
                SQLTopN sqlTopN = new SQLTopN();
                sqlTopN.setSql(rset.getString(1));
                sqlTopN.setUser(rset.getString(2));
                sqlTopN.setHost(rset.getString(3));
                sqlTopN.setSchema(rset.getString(4));
                sqlTopN.setTables(rset.getString(5));
                sqlTopN.setValue(rset.getLong(6));
                sqlTopNs.add(sqlTopN);
            }
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
        }finally {
            try {
                if(stmt !=null){
                    stmt.close();
                }
                if (rset !=null){
                    rset.close();
                }
            } catch (SQLException e) {
                LOGGER.error(e.getMessage());
            }
        }
        return sqlTopNs;
    }


     /**
     * 定期做sql stat 分析汇总入库
     */
    public ArrayList<SQLRecordSub> bySqlTypeSummaryPeriod(String sql){
        ArrayList<SQLRecordSub> arrayList = new ArrayList<SQLRecordSub>();

         final Connection h2DBConn =
                H2DBMonitorManager.getH2DBMonitorManager().getH2DBMonitorConn();
        Statement stmt = null;
        ResultSet rset = null;
        try {
            stmt = h2DBConn.createStatement();
            rset = stmt.executeQuery(sql);
            while (rset.next()){
                SQLRecordSub sqlRecord = new SQLRecordSub();
                sqlRecord.setOriginalSql(rset.getString(1));
                sqlRecord.setUser(rset.getString(2));
                sqlRecord.setHost(rset.getString(3));
                sqlRecord.setSchema(rset.getString(4));
                sqlRecord.setTables(rset.getString(5));
                sqlRecord.setSqlType(rset.getInt(6));
                sqlRecord.setResultRows(rset.getLong(7));
                sqlRecord.setExeTimes(rset.getLong(8));
                sqlRecord.setSqlexecTime(rset.getLong(9));
                arrayList.add(sqlRecord);
            }
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
        }finally {
            try {
                if(stmt !=null){
                    stmt.close();
                }
                if (rset !=null){
                    rset.close();
                }
            } catch (SQLException e) {
                LOGGER.error(e.getMessage());
            }
        }
        return arrayList;
    }

    /**
     * 定期转存数据到磁盘db中
     * @param expiredTime
     */
    private void dumpToDiskDb(long expiredTime){
        /**
         * 内存源数据库连接
         */
        final Connection h2DBConnSrc =
                H2DBMonitorManager.getH2DBMonitorManager().getH2DBMonitorConn();

        Statement stmt = null;
        ResultSet rset = null;
        String sql = "select * from t_sqlstat where lastaccess_t <=" +  expiredTime;

        try {

            stmt = h2DBConnSrc.createStatement();
            rset = stmt.executeQuery(sql);

            while (rset.next()){

                SQLHistoryRecord sqlHistoryRecord = new SQLHistoryRecord();
                sqlHistoryRecord.setOriginalSQL(rset.getString(1));
                sqlHistoryRecord.setModifiedSQL(rset.getString(2));
                sqlHistoryRecord.setUser(rset.getString(3));
                sqlHistoryRecord.setHost(rset.getString(4));
                sqlHistoryRecord.setSchema(rset.getString(5));
                sqlHistoryRecord.setTables(rset.getString(6));
                sqlHistoryRecord.setSqlType(rset.getInt(7));
                sqlHistoryRecord.setResultRows(rset.getLong(8));
                sqlHistoryRecord.getExecutionTimes().set(rset.getLong(9));
                sqlHistoryRecord.setStartTime(rset.getLong(10));
                sqlHistoryRecord.setEndTime(rset.getLong(11));
                sqlHistoryRecord.setSqlExecTime(rset.getLong(12));
                sqlHistoryRecord.setLastAccessedTimestamp(rset.getLong(13));
                SQLFirewallServer.getUpdateH2DBService().
                        submit(new SQLFirewallServer.Task<SQLHistoryRecord>(sqlHistoryRecord,OP_UPATE));
            }

        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
        }finally {
            try {
                if(stmt !=null){
                    stmt.close();
                }
                if (rset !=null){
                    rset.close();
                }
            } catch (SQLException e) {
                LOGGER.error(e.getMessage());
            }
        }
        return;
    }

    /**
     * 定期调用，删除过期的sql record。
     * @param expiredTime
     */
    private void deleteExpiredSqlStat(long expiredTime,String tableName){
            Connection h2DBConn = null;

            if (tableName.equals(H2DBManager.getSqlRecordTableName())) {
                h2DBConn = H2DBManager.getH2DBManager().getH2DBConn();
            }else {
                h2DBConn = H2DBMonitorManager.getH2DBMonitorManager().getH2DBMonitorConn();
            }
            Statement stmt = null;
            ResultSet rset = null;
            String sql = "delete from " +tableName+ " where lastaccess_t <=" +  expiredTime;
            try {
                stmt = h2DBConn.createStatement();
                stmt.execute(sql);
            } catch (SQLException e) {
                LOGGER.error(e.getMessage());
            } finally {
                try {

                    if (stmt != null) {
                        stmt.close();
                    }

                    if (rset != null) {
                        rset.close();
                    }

                } catch (SQLException e) {
                    LOGGER.error(e.getMessage());
                }
            }
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
                    heartbeatInfo.setHost(ds.getConfig().getIp() + ":" + ds.getConfig().getPort());
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


        FirewallConfig firewallConfig  = MycatServer.getInstance().getConfig().getFirewall();
        for (int i = 0; i < SystemParameter.SQLFIREWALL_PARAM.length ;i++) {
            String sysParam[] = SystemParameter.SQLFIREWALL_PARAM[i];
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
                method = firewallConfig.getClass()
                        .getMethod("get" + upperName);
                value = method.invoke(firewallConfig);
                sysParameter.setVarValue(String.valueOf(value));
            } catch (Exception e) {
                LOGGER.error(e.getMessage());
            }

            sysParameter.update();
        }

        for (int i = 0; i <SystemParameter.SQLFIREWALL_PARAM_BOOL.length ; i++) {
            String sysParamBool[] = SystemParameter.SQLFIREWALL_PARAM_BOOL[i];
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
                method = firewallConfig.getClass()
                        .getMethod("is" + upperName);
                value = method.invoke(firewallConfig);
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
    private void updateMemoryInfo() {

        BufferPool bufferPool = MycatServer.getInstance().getBufferPool();
        MyCatMemory myCatMemory = MycatServer.getInstance().getMyCatMemory();
        ConcurrentHashMap<Long,Long> bufferPooMap = null;
        ConcurrentHashMap<Long ,Long> mergeMemoryMap = null;

        /**
         * 更新memory info
         */
        Runtime rt = Runtime.getRuntime();
        long total = rt.totalMemory();
        long max = rt.maxMemory();
        long used = (total - rt.freeMemory());
        MemoryInfo memoryInfo = new MemoryInfo();
        memoryInfo.setThreadId(mainThreadId);
        memoryInfo.setThreadName("Main-" + mainThreadId);
        memoryInfo.setMemoryType(MemoryType.ONHEAP.getName());
        memoryInfo.setUsed(used);
        memoryInfo.setMax(max);
        memoryInfo.setTotal(total);
        memoryInfo.update();
        /**
         * 更新 结果集汇聚 Direct Memory
         */
        used = 0;
        DirectMemoryInfo mergeDirectMemoryInfo = new DirectMemoryInfo();
        /**id =1 表示是结果汇聚堆外内存*/
        mergeDirectMemoryInfo.setId(1);
        mergeDirectMemoryInfo.setMemoryType(MemoryType.MergeMemory.getName());
        mergeDirectMemoryInfo.setMax(myCatMemory.getResultSetBufferSize());
        /**统计当前结果集汇聚使用的内存*/
        mergeMemoryMap =
                myCatMemory.getResultMergeMemoryManager().getDirectMemorUsage();
        for (Map.Entry<Long,Long> entry:mergeMemoryMap.entrySet()) {
            used += entry.getValue();
        }
        mergeDirectMemoryInfo.setUsed(used);
        mergeDirectMemoryInfo.setTotal(Platform.getMaxDirectMemory());
        mergeDirectMemoryInfo.update();

        /**
         * 更新 网络packe 处理 Direct Memory
         */


        used = 0;
        DirectMemoryInfo netDirectMemoryInfo = new DirectMemoryInfo();
        /**id =2 表示是网络堆外内存*/
        netDirectMemoryInfo.setId(2);
        netDirectMemoryInfo.setMemoryType(MemoryType.NetMemory.getName());


        /**统计当前表示是网络处理使用的堆外内存*/
        bufferPooMap = bufferPool.getMemoryUsage();
        for (Map.Entry<Long,Long> entry:bufferPooMap.entrySet()) {
            used += entry.getValue();
        }
        netDirectMemoryInfo.setUsed(used);

        netDirectMemoryInfo.setMax(bufferPool.capacity()*bufferPool.getChunkSize());
        netDirectMemoryInfo.setTotal(Platform.getMaxDirectMemory());
        netDirectMemoryInfo.update();

        /**
         * 网络packe 处理 Direct Memory 不够时，会创建临时on heap ByteBuffer
         */

        int tempByteBufferCount = bufferPool.getNewTempBufferByteCreated().get();
        if(tempByteBufferCount > 0) {
            DirectMemoryInfo tempOnHeapMemoryInfo = new DirectMemoryInfo();
            /**id =3 表示是网络处理时，堆外内存不够时，使用on heap 临时ByteBuffer*/
            tempOnHeapMemoryInfo.setId(3);
            tempOnHeapMemoryInfo.setMemoryType(MemoryType.ONHEAP.getName());
            tempOnHeapMemoryInfo.setUsed(tempByteBufferCount*bufferPool.getChunkSize());
            tempOnHeapMemoryInfo.setMax(rt.maxMemory());
            tempOnHeapMemoryInfo.setTotal(rt.totalMemory());
            tempOnHeapMemoryInfo.update();
        }



        /**
         * 输出详细的 Direct Memory
         */
        /**输出当前网络处理使用的内存详细情况*/
        mergeMemoryMap =
                myCatMemory.getResultMergeMemoryManager().getDirectMemorUsage();

        if (mergeMemoryMap.size() == 0){
            DirectMemoryDetailInfo.delete(MemoryType.MergeMemory.getName());
        }

        for (Map.Entry<Long,Long> entry:mergeMemoryMap.entrySet()) {
            DirectMemoryDetailInfo  dmd = new DirectMemoryDetailInfo();
            LOGGER.info("use megre :" + entry.getValue());
            dmd.setThreadId(entry.getKey());
            dmd.setMemoryType(MemoryType.MergeMemory.getName());
            dmd.setUsed(entry.getValue());
            dmd.update();
        }

        /**输出当前结果集汇聚使用的内存详细情况*/
        bufferPooMap = bufferPool.getMemoryUsage();
        if (bufferPooMap.size() == 0){
            DirectMemoryDetailInfo.delete(MemoryType.NetMemory.getName());
        }

        for (Map.Entry<Long,Long> entry:bufferPooMap.entrySet()) {
            DirectMemoryDetailInfo  dmd = new DirectMemoryDetailInfo();
            dmd.setThreadId(entry.getKey());
            dmd.setMemoryType(MemoryType.NetMemory.getName());
            dmd.setUsed(entry.getValue());
            dmd.update();
        }
    }

    /**
     * 定时检查schema各分片节点表结构是否一致
     */
    public void CheckTableStructureConsistency()
    {
        Set<String>  schemaList =  MycatServer.getInstance().getConfig().getSchemas().keySet();
        for (String schema: schemaList) {
            CheckTableStructureConsistencyHandler handler = new CheckTableStructureConsistencyHandler(schema,null,false);
            try {
                handler.handle();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 更新网络连接相关新。thread pool， connection pool， client connection等
     */

    private void updateNetConnection(){
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
                threadPoolInfo.setCompletedTask(exec.getCompletedTaskCount());
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
