package org.opencloudb.backend.infoschema;

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.PhysicalDBPool;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.memory.MyCatMemory;
import org.opencloudb.memory.unsafe.map.UnsafeFixedWidthAggregationMap;
import org.opencloudb.memory.unsafe.memory.mm.DataNodeMemoryManager;
import org.opencloudb.memory.unsafe.memory.mm.MemoryManager;
import org.opencloudb.memory.unsafe.row.BufferHolder;
import org.opencloudb.memory.unsafe.row.StructType;
import org.opencloudb.memory.unsafe.row.UnsafeRow;
import org.opencloudb.memory.unsafe.row.UnsafeRowWriter;
import org.opencloudb.memory.unsafe.utils.MycatPropertyConf;
import org.opencloudb.memory.unsafe.utils.sort.PrefixComparator;
import org.opencloudb.memory.unsafe.utils.sort.PrefixComparators;
import org.opencloudb.memory.unsafe.utils.sort.RowPrefixComputer;
import org.opencloudb.memory.unsafe.utils.sort.UnsafeExternalRowSorter;
import org.opencloudb.mpp.ColMeta;
import org.opencloudb.mpp.OrderCol;
import org.opencloudb.mpp.PackWraper;
import org.opencloudb.mysql.MySQLMessage;
import org.opencloudb.sqlengine.AllJobFinishedListener;
import org.opencloudb.sqlengine.EngineCtx;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 外部模块调用入口
 *
 * @author zagnix
 * @version 1.0
 * @create 2016-09-09 10:23
 */


/**
 *  select * from COLUMNS where TABLE_SCHEMA != 'information_schema' limit 1 \G;
 *************************** 1. row ***************************
 TABLE_CATALOG: def
 TABLE_SCHEMA: exp1
 TABLE_NAME: tl_kafka_log
 COLUMN_NAME: id
 COLUMN_DEFAULT: NULL
 IS_NULLABLE: NO
 DATA_TYPE: varchar
 int CHARACTER_MAXIMUM_LENGTH: 64
 NUMERIC_PRECISION: NULL
 NUMERIC_SCALE: NULL
 DATETIME_PRECISION: NULL
 CHARACTER_SET_NAME: utf8
 COLLATION_NAME: utf8_general_ci
 COLUMN_TYPE: varchar(64)
 COLUMN_KEY: PRI
 EXTRA:NULL
 PRIVILEGES: select,insert,update,references
 */
/**
 *  select * from STATISTICS limit 1\G;
 *************************** 1. row ***************************
 TABLE_CATALOG: def
 TABLE_SCHEMA: exp1
 TABLE_NAME: tl_kafka_log
 NON_UNIQUE: 0
 INDEX_SCHEMA: exp1
 INDEX_NAME: PRIMARY
 SEQ_IN_INDEX: 1
 COLUMN_NAME: id
 COLLATION: A
 CARDINALITY: 0
 SUB_PART: NULL
 PACKED: NULL
 NULLABLE:
 INDEX_TYPE: BTREE
 1 row in set (0.02 sec)
 */

/**
  mysql> desc STATISTICS;
 +---------------+---------------+------+-----+---------+-------+
 | Field         | Type          | Null | Key | Default | Extra |
 +---------------+---------------+------+-----+---------+-------+
 | TABLE_CATALOG | varchar(512)  | NO   |     |         |       |
 | TABLE_SCHEMA  | varchar(64)   | NO   |     |         |       |
 | TABLE_NAME    | varchar(64)   | NO   |     |         |       |
 | NON_UNIQUE    | bigint(1)     | NO   |     | 0       |       |
 | INDEX_SCHEMA  | varchar(64)   | NO   |     |         |       |
 | INDEX_NAME    | varchar(64)   | NO   |     |         |       |
 | SEQ_IN_INDEX  | bigint(2)     | NO   |     | 0       |       |
 | COLUMN_NAME   | varchar(64)   | NO   |     |         |       |
 | COLLATION     | varchar(1)    | YES  |     | NULL    |       |
 | CARDINALITY   | bigint(21)    | YES  |     | NULL    |       |
 | SUB_PART      | bigint(3)     | YES  |     | NULL    |       |
 | PACKED        | varchar(10)   | YES  |     | NULL    |       |
 | NULLABLE      | varchar(3)    | NO   |     |         |       |
 | INDEX_TYPE    | varchar(16)   | NO   |     |         |       |
 | COMMENT       | varchar(16)   | YES  |     | NULL    |       |
 | INDEX_COMMENT | varchar(1024) | NO   |     |         |       |
 +---------------+---------------+------+-----+---------+-------+
 */

/**
 *desc COLUMNS;
 +--------------------------+---------------------+------+-----+---------+-------+
 | Field                    | Type                | Null | Key | Default | Extra |
 +--------------------------+---------------------+------+-----+---------+-------+
 | TABLE_CATALOG            | varchar(512)        | NO   |     |         |       |
 | TABLE_SCHEMA             | varchar(64)         | NO   |     |         |       |
 | TABLE_NAME               | varchar(64)         | NO   |     |         |       |
 | COLUMN_NAME              | varchar(64)         | NO   |     |         |       |
 | ORDINAL_POSITION         | bigint(21) unsigned | NO   |     | 0       |       |
 | COLUMN_DEFAULT           | longtext            | YES  |     | NULL    |       |
 | IS_NULLABLE              | varchar(3)          | NO   |     |         |       |
 | DATA_TYPE                | varchar(64)         | NO   |     |         |       |
 | CHARACTER_MAXIMUM_LENGTH | bigint(21) unsigned | YES  |     | NULL    |       |
 | CHARACTER_OCTET_LENGTH   | bigint(21) unsigned | YES  |     | NULL    |       |
 | NUMERIC_PRECISION        | bigint(21) unsigned | YES  |     | NULL    |       |
 | NUMERIC_SCALE            | bigint(21) unsigned | YES  |     | NULL    |       |
 | DATETIME_PRECISION       | bigint(21) unsigned | YES  |     | NULL    |       |
 | CHARACTER_SET_NAME       | varchar(32)         | YES  |     | NULL    |       |
 | COLLATION_NAME           | varchar(32)         | YES  |     | NULL    |       |
 | COLUMN_TYPE              | longtext            | NO   |     | NULL    |       |
 | COLUMN_KEY               | varchar(3)          | NO   |     |         |       |
 | EXTRA                    | varchar(30)         | NO   |     |         |       |
 | PRIVILEGES               | varchar(80)         | NO   |     |         |       |
 | COLUMN_COMMENT           | varchar(1024)       | NO   |     |         |       |
 +--------------------------+---------------------+------+-----+---------+-------+
 */
public class MySQLInfoSchemaProcessor implements AllJobFinishedListener {

    public static final Logger LOGGER =
            Logger.getLogger(MySQLInfoSchemaProcessor.class);

    private final EngineCtx ctx;
    private int maxjobs = 0;
    private AtomicInteger integer = null;

    private static final String[] MYSQL_INFO_SCHEMA_TCOLUMNS = new String[] {
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "COLUMN_NAME",
            "COLUMN_DEFAULT",
            "IS_NULLABLE",
            "DATA_TYPE",
            "CHARACTER_MAXIMUM_LENGTH",
            "NUMERIC_PRECISION",
            "NUMERIC_SCALE",
            "DATETIME_PRECISION",
            "CHARACTER_SET_NAME",
            "COLLATION_NAME",
            "COLUMN_TYPE",
            "COLUMN_KEY",
            "EXTRA",
            "PRIVILEGES"};


    private static final String[] MYSQL_INFO_SCHEMA_TSTATISTICS = new String[] {
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "INDEX_NAME",
            "SEQ_IN_INDEX",
            "NON_UNIQUE",
            "INDEX_SCHEMA",
            "SEQ_IN_INDEX",
            "COLUMN_NAME",
            "COLLATION",
            "CARDINALITY",
            "SUB_PART",
            "PACKED",
            "NULLABLE",
            "INDEX_TYPE"};
    /**
     * rowData缓存队列
     */
    protected BlockingQueue<PackWraper> packs = new LinkedBlockingQueue<PackWraper>();

    /**
     * 结束包
     */
    private PackWraper END_FLAG_PACK = new PackWraper();

    /**
     * 由于不需要排序相关的属性都为null即可
     */
    private OrderCol[] orderCols = null;
    private StructType schema = null;
    private UnsafeExternalRowSorter.PrefixComputer prefixComputer = null;
    private PrefixComparator prefixComparator = null;
    private DataNodeMemoryManager dataNodeMemoryManager = null;

    /**
     * sorter需要的上下文环境
     */
    private final MyCatMemory myCatMemory;
    private final MemoryManager memoryManager;
    private final MycatPropertyConf conf;

    /**
     * 全局merge，排序器
     */
    private UnsafeExternalRowSorter mergeResult = null;

    /**
     * 标志业务线程是否启动了？
     */
    protected final AtomicBoolean running = new AtomicBoolean(false);

    private FutureTask<Iterator<UnsafeRow>> futureTask = null;

    private static  CountDownLatch countDownLatch = new CountDownLatch(1);

    private final String execSql;
    private final String [] colNames;
    private final String information_schema_db;
    private int dataHostSize;

    public MySQLInfoSchemaProcessor(String information_schema_db,int dataHostSize,String sql,String [] cols) throws IOException {

        this.information_schema_db = information_schema_db;
        this.ctx = new EngineCtx(null);
        this.dataHostSize = dataHostSize;
        this.maxjobs = dataHostSize;
        this.integer = new AtomicInteger(0);
        this.myCatMemory = MycatServer.getInstance().getMyCatMemory();
        this.memoryManager = myCatMemory.getResultMergeMemoryManager();
        this.conf = myCatMemory.getConf();
        this.execSql = sql;

        /**
         * TODO 后面通过druid 解析sql中的列名，填充这个colNames。
         */
        this.colNames = cols;
        Map<String, ColMeta> columToIndx = new HashMap<String, ColMeta>(
                this.colNames.length);
        /**
         * 1.schema
         */
        schema = new StructType(columToIndx, this.colNames.length);
        schema.setOrderCols(orderCols);

        /**
         * 2 .PrefixComputer
         */
        prefixComputer = new RowPrefixComputer(schema);
        /**
         * 3 .PrefixComparator 默认是ASC，可以选择DESC
         */
        prefixComparator = PrefixComparators.LONG;
        dataNodeMemoryManager = new DataNodeMemoryManager(memoryManager,Thread.currentThread().getId());

        mergeResult = new UnsafeExternalRowSorter(
                dataNodeMemoryManager,
                myCatMemory,
                schema,
                prefixComparator,
                prefixComputer,
                conf.getSizeAsBytes("mycat.buffer.pageSize", "1m"),false,false);

        TaskResult task = new TaskResult();
        futureTask = new FutureTask<Iterator<UnsafeRow>>(task);
    }


    /**
     * 将SQL发送到后端
     */
    public Iterator<UnsafeRow> processSQL() throws Exception {

        MySQLInfoSchemaResultHandler schemaResultHandler =
                new MySQLInfoSchemaResultHandler(this);

        Map<String,SchemaConfig>  dataNodeMaps = MycatServer.getInstance().getConfig().getSchemas();

        SchemaConfig config =  dataNodeMaps.get(information_schema_db);

        String[] dataNodes = config!=null?config.getAllDataNodeStrArr():null;

        if((dataNodes == null) || (this.dataHostSize != dataNodes.length)){
            throw new Exception("Information_schema 's DataNode size must equal to datahost size");
        }

        ctx.executeNativeSQLParallJob(dataNodes,this.execSql,schemaResultHandler);

        while (countDownLatch.getCount() != 0){
            Thread.sleep(2000);
        }

        return futureTask.get();
    }

    @Override
    public void onAllJobFinished(EngineCtx ctx) {
        LOGGER.error("onAllJobFinished");
    }

    public void endJobInput(String dataNode, boolean failed){
        synchronized (this) {
            if (integer.incrementAndGet() >= maxjobs) {
                ctx.endJobInput();
                addPack(END_FLAG_PACK);
                LOGGER.info("All Jobs Finished " + integer.get());
                integer.getAndSet(0);
            }
        }
    }

    protected final boolean addPack(final PackWraper pack){
        packs.add(pack);
        if(running.get()){
            return false;
        }
        final MycatServer server = MycatServer.getInstance();
        server.getBusinessExecutor().submit(futureTask);
        return true;
    }


    private class TaskResult implements Callable<Iterator<UnsafeRow>>{
        @Override
        public  Iterator<UnsafeRow> call() throws Exception {
            Iterator<UnsafeRow> iters = null;

            if (!running.compareAndSet(false, true)) {
                return null;
            }

            try {
                for (; ;) {
                    final PackWraper pack = packs.poll();

                    if(pack == null){
                        continue;
                    }

                    if (pack == END_FLAG_PACK) {
                       iters = mergeResult.sort();
                        countDownLatch.countDown();
                        LOGGER.info("countDownLatch" + countDownLatch.getCount());
                        break;
                    }

                    /**
                     *构造一行row，将对应的col填充.
                     * 分别构造 key，value.
                     */
                    UnsafeRow value = new UnsafeRow(colNames.length);
                    BufferHolder  bufferHolder = new BufferHolder(value,0);
                    UnsafeRowWriter unsafeRowWriter = new UnsafeRowWriter(bufferHolder,colNames.length);
                    bufferHolder.reset();
                    MySQLMessage mm = new MySQLMessage(pack.rowData);
                    mm.readUB3();
                    mm.read();
                    for (int i = 0; i < colNames.length; i++) {
                            byte[] colValue = mm.readBytesWithLength();
                            if (colValue != null)
                                unsafeRowWriter.write(i,colValue);
                            else {
                                value.setNullAt(i);
                            }
                    }
                    value.setTotalSize(bufferHolder.totalSize());

                    mergeResult.insertRow(value);
                }
            } catch (final Exception e) {
                countDownLatch.countDown();
                LOGGER.error(e.getMessage());
            } finally {
                running.set(false);
            }
            return iters;
        }
    }

    public void cleanup (){
        if(mergeResult !=null)
            mergeResult.cleanupResources();
    }
}
