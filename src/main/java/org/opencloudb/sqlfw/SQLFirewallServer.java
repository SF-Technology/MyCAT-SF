package org.opencloudb.sqlfw;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.wall.Violation;
import com.alibaba.druid.wall.WallCheckResult;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.FirewallConfig;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.monitor.SQLRecord;
import org.opencloudb.net.AbstractConnection;
import org.opencloudb.net.FrontendConnection;
import org.opencloudb.parser.druid.DruidShardingParseInfo;
import org.opencloudb.server.ServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SQL FrieWall Server
 *
 * @author zagnix
 * @create 2016-10-20 13:59
 */

public class SQLFirewallServer {

    private final static Logger LOGGER = LoggerFactory.getLogger(SQLFirewallServer.class);

    /**
     * 默认value 超过DEFAULT_TIMEOUT时间，将写到H2DB中
     */
    public static final long DEFAULT_TIMEOUT = 300 * 1000;



    /**
     * SQL黑名单常驻内存，有sql添加进来时，定时刷到保存放到H2DB中
     */
    private static final ConcurrentHashMap<Integer,String> sqlBlackListMap
            = new ConcurrentHashMap<Integer,String>();

    private static final ConcurrentHashMap<String,SQLRecord> sqlRecordMap
            = new ConcurrentHashMap<String,SQLRecord>();


    /**
     * 同步
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock rLock = lock.readLock();
    private final Lock wLock = lock.writeLock();
    private SQLFirewall sqlFirewall = null;
    private  AtomicInteger sqlId;
    public static final   int OP_UPATE = 1;
    public static final  int OP_DEL = 2;
    public static final  int OP_UPATE_ROW = 3;

    /**
     * 异步线程，用来将数据更新到H2DB.
     */
    private static final ThreadFactory threadFactory =
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("async-op-h2db ").build();

    private static final ExecutorService updateH2DBService =
            Executors.newSingleThreadExecutor(threadFactory);


    private static final ThreadFactory scheduleFactory =
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("schedule fixed rate").build();

    private static final ScheduledExecutorService scheduleAtFixedRateExecutor =
            new ScheduledThreadPoolExecutor(1,scheduleFactory);

    private FirewallConfig firewallConf = null;


    /**
     * Task 主要将Key对应的value超时写入H2DB中
     * @param <V>
     */
    public static class Task<V extends H2DBInterface> implements Runnable{
        private V value = null;
        private int op = 0;

        public Task( V value,int op){
            this.value = value;
            this.op = op;
        }
        @Override
        public void run() {
            if (value != null) {
                switch (op){
                    case SQLFirewallServer.OP_UPATE:
                        value.update();
                        break;
                    case SQLFirewallServer.OP_DEL:
                        value.delete();
                        break;
                    case SQLFirewallServer.OP_UPATE_ROW:
                        value.update_row();
                        break;
                    default:
                        LOGGER.info("op err");
                        break;
                }
            }
        }
    }

    public SQLFirewallServer(){

        firewallConf = MycatServer.getInstance().getConfig().getFirewall();
        /**
         * 初始化sql_id
         * 从数据sql_backlist 查询最大sql_id作为初始值。
         */

        sqlId = new AtomicInteger(initSqlId());

        LOGGER.info("init sql blacklist sql_id :" + sqlId.get());

        /**
         * 加载全部sql黑名单到sqlBackListMap中
         */
        loadSQLBlackList();

        /**
         * druid wall 功能
         */
        sqlFirewall = SQLFirewall.getSqlFirewall();

        /**
         * 定时移除sqlRecordMap过期的元素
         */
        scheduleAtFixedRateExecutor.scheduleAtFixedRate(new Runnable(){
            @Override
            public void run() {
                for (String key:sqlRecordMap.keySet()) {
                    SQLRecord sqlRec = sqlRecordMap.get(key);
                    if((System.currentTimeMillis()-sqlRec.getLastAccessedTimestamp())
                            > firewallConf.getMaxAllowExecuteUnitTime()*1000){
                        if(LOGGER.isDebugEnabled()){
                            LOGGER.debug("sql record:  " +  key + "will remove from sql record map......");
                        }
                        sqlRecordMap.remove(key);
                    }
                }
            }
        },0,firewallConf.getMaxAllowExecuteUnitTime()*2,TimeUnit.SECONDS);
    }

    /**
     * 加载全部sql黑名单到sqlBackListMap中
     */
    public void loadSQLBlackList(){
        final Connection h2DBConn = H2DBManager.getH2DBManager().getH2DBConn();;
        Statement stmt = null;
        ResultSet rset = null;
        try {
            String sql = "select * from sql_blacklist";
            stmt = h2DBConn.createStatement();
            rset = stmt.executeQuery(sql);
            while (rset.next()){
                if (!sqlBlackListMap.containsKey(rset.getInt(1)))
                        sqlBlackListMap.put(rset.getInt(1),rset.getString(2));
            }
        } catch (SQLException e) {
            LOGGER.error("1:" + e.getSQLState());
        }finally {
            try {
                if(stmt !=null){
                    stmt.close();
                }
                if (rset !=null){
                    rset.close();
                }
            } catch (SQLException e) {
                LOGGER.error("2:" + e.getSQLState());
            }
        }
    }

    /**
     * 添加一条SQL 到 SQL黑名单中
     * 加入sqlBackList中，同时异步更新到H2DB中。
     * @param sql
     * @return
     */
    public boolean addSqlToBlacklist(String sql){

        if (sqlBlackListMap.contains(sql))
            return false;

        final int id = getIncrementSqlId();

        sqlBlackListMap.put(id,sql);

        /**
         * 异步操作数据库
         */
        try {
            wLock.lock();
            SQLBlackList sqlBackList = new SQLBlackList();
            sqlBackList.setId(id);
            sqlBackList.setOriginalSQL(sql.replace("'","\'"));
            updateH2DBService.submit(new Task<SQLBlackList>(sqlBackList,OP_UPATE));
        }finally {
            wLock.unlock();
        }

        return true;
    }

    /**
     * 从SQL黑名单中移除一条SQL
     * 从sqlBackList移除，同时异步删除H2DB中sql记录
     * @param id
     * @return
     */
    public boolean removeSqlfromBackList(int id){
        String sql  = null;
        if(sqlBlackListMap.containsKey(id)){
            sql = (String) sqlBlackListMap.remove(id);
        }else{
            return false;
        }
        sqlRecordMap.remove(sql);
        return true;
    }

    /**
     * 添加一条sql执行记录
     * @param originalSQL
     * @param modifiedSQL
     * @return
     */
    public boolean AddSQLRecord(String originalSQL,String modifiedSQL){

        if (sqlBlackListMap.containsValue(originalSQL)){
            return false;
        }

        boolean flag = false;
        SQLRecord sqlRecord = null;

        try {
            wLock.lock();

            long t = System.currentTimeMillis();
            if (sqlRecordMap.containsKey(originalSQL)){
                sqlRecord = sqlRecordMap.get(originalSQL);
                sqlRecord.getExecutionTimes().incrementAndGet();
            }else {
                sqlRecord = new SQLRecord(DEFAULT_TIMEOUT);
                sqlRecord.setStartTime(t);
                sqlRecord.setEndTime(t);
                sqlRecord.setOriginalSQL(originalSQL);
                sqlRecord.setModifiedSQL(modifiedSQL);
                sqlRecord.getExecutionTimes().lazySet(1);
            }
            sqlRecord.setLastAccessedTimestamp(t);
            sqlRecordMap.put(originalSQL,sqlRecord);
            flag = true;
        }finally {
            wLock.unlock();
        }
        return flag;
    }

    /**
     * 根据sql获取对应的sql执行信息
     * @param sql
     * @return
     */
    public SQLRecord getSQLRecord(String sql){
        return sqlRecordMap.get(sql);
    }


    /**
     * SQL黑名单匹配，如果sql存在sql backlist返回true，否则false
     * @param sql
     * @return true sccuess. false , failed.
     */
    public boolean sqlMatcher(String sql){

        SQLRecord sqlRecord = sqlRecordMap.get(sql);

        if (sqlRecord != null && LOGGER.isDebugEnabled()){
            LOGGER.debug("Sql  Record  " +   sqlRecord.toString());
        }


        /**
         * 结果集超过了max rows, 则动态添加到SQL黑名单中
         */
        if(sqlRecord != null) {
            if (sqlRecord.getResultRows() >=
            		firewallConf.getMaxAllowResultRow()) {

                LOGGER.error(sql + " of result rows more than maximum value "
                        + firewallConf.getMaxAllowResultRow());

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(sql + " of result rows more than maximum value "
                            + firewallConf.getMaxAllowResultRow());
                }

                addSqlToBlacklist(sql);
                sqlRecordMap.remove(sql);
                return true;
            }

            /**
             * 单位为s,一条sql执行的时间，超过了 max time, 则动态加入SQL黑名单中
             */
            long sqlExecuteTime = sqlRecord.getSqlExecTime();

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("SQL execute time  " + sqlExecuteTime + " s");
            }

            if (sqlExecuteTime > firewallConf.getMaxAllowExecuteSqlTime()) {

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(sql + " execution time more than maximum value "
                            + firewallConf.getMaxAllowExecuteSqlTime());
                }

                long count = sqlRecord.getCountInMaxAllowExecuteSqlTime().incrementAndGet();
                if(count > firewallConf.getCountInMaxAllowExecuteSqlTime()) {
                    addSqlToBlacklist(sql);
                    sqlRecordMap.remove(sql);
                    return true;
                }
                return false;
            }

            /**
             * maxAllowExecuteUnitTime's 内最大允许执行次数，超过了动态添加到SQL黑名单中
             */
            long interval = (System.currentTimeMillis() - sqlRecord.getLastAccessedTimestamp()) / 1000;

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Unit Time " + interval + " s ");
            }

            if ((interval < firewallConf.getMaxAllowExecuteUnitTime()) &&
                    (sqlRecord.getExecutionTimes().get() > firewallConf.getMaxAllowExecuteTimes())) {


                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("In Unit Time " + firewallConf.getMaxAllowExecuteUnitTime() + " , " +
                            sql + "  execution times more than maximum value " + firewallConf.getMaxAllowExecuteTimes());
                }

                addSqlToBlacklist(sql);
                sqlRecordMap.remove(sql);
                return true;
            }

        }


        /**
         * 基于正则表达式匹配
         */
        boolean enableRegEx = firewallConf.isEnableRegEx();

        if (enableRegEx) {

            for (String regex : sqlBlackListMap.values()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("regEx  : " + regex);
                    LOGGER.debug("regEx matcher sql : " + sql);
                }

                if (Pattern.compile(regex, Pattern.CASE_INSENSITIVE)
                        .matcher(sql).matches()) {
                    return true;
                }
            }
        }

        /**
         * 简单的直接匹配效率高
         */
        return sqlBlackListMap.containsValue(sql);
    }

    /**
     *  SQL 语句check
     *
     * @param sql SQL语句
     * @param sc  网络连接
     * @return false 表示被拦截，true 通过
     */

    public boolean checkSql(String sql,AbstractConnection sc,int enableSQLFirewall){

        boolean falg = true;

        WallCheckResult result =
                sqlFirewall.getProvider().check(sql);

        if (result.getViolations().size() > 0){
            /**
             * 得到SQL语句检查结果
             */
            Violation violation = result.getViolations().get(0);

            if(enableSQLFirewall == 1){
                /**
                 * 拦截SQL，并将出错信息返回给客户端
                 */
                if(sc instanceof ServerConnection){
                    ((FrontendConnection) sc).writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND,
                            violation.getMessage() + ",  sql==> " + sql);
                }

            }else if(enableSQLFirewall == 2){
                /**
                 * 不拦截SQL，将sql记录在拦截reporter中
                 */
                recordSQLReporter(sql,violation.getMessage().toUpperCase());
            }else if (enableSQLFirewall == 0){
                LOGGER.warn("'" + sql.toUpperCase() + "' that is ".toUpperCase() + violation.getMessage().toUpperCase());
            }

            falg  = false;
        }

        return falg;
    }

    public void updateSqlRecord(String originalSQL,SQLRecord sqlRecord){
        sqlRecordMap.put(originalSQL,sqlRecord);
    }

    /**
     * 从sql_backlist 取 sql_id 的最大值
     * @return
     */
    public int initSqlId(){
        int id = 0;
        Statement stmt = null;
        ResultSet rset = null;
        try {
            stmt = H2DBManager.getH2DBManager().getH2DBConn().createStatement();
            rset = stmt.executeQuery("select max(sql_id) from sql_blacklist");
            if (rset.next()){
                id = rset.getInt(1);
            }
        } catch (SQLException e) {
           LOGGER.error("initSqlId 1" + e.getSQLState());
        }finally {
                try {
                    if(rset != null){
                        rset.close();
                    }

                    if (stmt != null){
                        stmt.close();
                    }

                } catch (SQLException e) {
                    LOGGER.error("initSqlId 2" + e.getSQLState());
                }
        }

        return id;
    }

    /**
     * ++sqlId
     * @return
     */
    public int getIncrementSqlId() {
        return sqlId.incrementAndGet();
    }

    public static ConcurrentHashMap<String,SQLRecord> getSqlRecordMap() {
        return sqlRecordMap;
    }

    public static ConcurrentHashMap<Integer, String> getSqlBlackListMap() {
        return sqlBlackListMap;
    }


    /**
     * 将拦截的sql写入H2DB.
     * @param sql
     * @param sqlMsg
     * @return
     */
    public boolean recordSQLReporter(String sql,String sqlMsg){
        boolean flag = true;
        try {
            wLock.lock();
            SQLReporter sqlReporter = new SQLReporter();
            sqlReporter.setSql(sql);
            sqlReporter.setSqlMsg(sqlMsg);
            updateH2DBService.submit(new Task<SQLReporter>(sqlReporter,OP_UPATE));
        }finally {
            wLock.unlock();
        }
        return flag;
    }


    /**
     * 告警 没有分片字段的 SQL
     * @param schema
     * @param statement
     * @param ctx
     * @param sql
     * */
    public void interceptSQL(SchemaConfig schema, SQLStatement statement,
                                       DruidShardingParseInfo ctx, String sql) {
        Map<String, String> tableAliasMap = new HashMap<String, String>();
        Set<String> colSets = new LinkedHashSet<String>();
        Set<String> conditionColSets = new LinkedHashSet<String>();

        if (ctx != null) {
            tableAliasMap = ctx.getTableAliasMap();
        }

        MySqlSchemaStatVisitor mySQLKVisitor = new MySqlSchemaStatVisitor();
        statement.accept(mySQLKVisitor);

        Iterator<TableStat.Column> c = mySQLKVisitor.getColumns().iterator();

        while (c.hasNext()) {
            TableStat.Column col = c.next();
            colSets.add(col.getName());
        }

        if (statement instanceof SQLInsertStatement
                || statement instanceof SQLUpdateStatement) {
            String tName = mySQLKVisitor.getCurrentTable();
            if (tableAliasMap.size() > 0 && tableAliasMap.containsKey(tName)) {
                tName = tableAliasMap.get(tName);
            }
            if (schema != null) {
                Map<String, TableConfig> map = schema.getTables();
                TableConfig tableConfig = map.get(tName.toUpperCase());
                if (tableConfig != null) {
                    String partitionColumn = tableConfig.getPartitionColumn();
                    if (partitionColumn != null && !conditionColSets.contains(partitionColumn)) {
                        recordSQLReporter(sql.replace("'",""),"no sharding key!!!!!");
                    }
                }
            }
        }
    }


    /**
     * 异步执行线程
     * @return
     */
    public static ExecutorService getUpdateH2DBService() {
        return updateH2DBService;
    }

}
