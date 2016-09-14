package org.opencloudb.backend.infoschema;


import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.mpp.PackWraper;
import org.opencloudb.sqlengine.AllJobFinishedListener;
import org.opencloudb.sqlengine.EngineCtx;
import org.opencloudb.sqlengine.SQLQueryResultListener;

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

public class MySQLInfoSchemaProcessor implements AllJobFinishedListener {

    public static final Logger LOGGER =
            Logger.getLogger(MySQLInfoSchemaProcessor.class);
    private final EngineCtx ctx;
    private int maxjobs = 0;
    private AtomicInteger integer = null;

    /**
     * rowData缓存队列
     */
    protected BlockingQueue<PackWraper> packs = new LinkedBlockingQueue<PackWraper>();

    /**
     * 结束包
     */
    private PackWraper END_FLAG_PACK = new PackWraper();

    /**
     * 标志业务线程是否启动了？
     */
    protected final AtomicBoolean running = new AtomicBoolean(false);

    private final String execSql;
    private final String [] colNames;
    private final String information_schema_db;
    private int dataHostSize;
    private final String dataNodes[];
    private HashMap<String,LinkedList<byte[]>> mapHostData;
    private final SQLQueryResultListener sqlQueryResultListener;
    private Task task = null;

    public MySQLInfoSchemaProcessor(String information_schema_db,
                                    int dataHostSize, String sql,
                                    String [] cols,SQLQueryResultListener sqlQueryResultListener) throws Exception {

        this.information_schema_db = information_schema_db;
        this.ctx = new EngineCtx(null);
        this.dataHostSize = dataHostSize;
        this.maxjobs = dataHostSize;
        this.integer = new AtomicInteger(0);
        this.execSql = sql;
        this.ctx.setAllJobFinishedListener(this);
        this.sqlQueryResultListener = sqlQueryResultListener;

        /**
         * TODO 后面通过druid 解析sql中的列名，填充这个colNames。
         */
        this.colNames = cols;
        Map<String,SchemaConfig>  dataNodeMaps = MycatServer.getInstance().getConfig().getSchemas();
        SchemaConfig config =  dataNodeMaps.get(information_schema_db);
        this.dataNodes = config!=null?config.getAllDataNodeStrArr():null;

        if((dataNodes == null) || (this.dataHostSize != dataNodes.length)){
            throw new Exception("Information_schema 's DataNode size must equal to datahost size");
        }

        mapHostData = new HashMap<String,LinkedList<byte[]>>(dataNodes.length);

        for (String host:dataNodes) {
            LinkedList<byte []> linkedlist = new LinkedList<byte[]>();
            mapHostData.put(host,linkedlist);
        }

      this.task  = new Task();

    }

    /**
     * 将SQL发送到后端,异步等待结果返回
     */
    public void processSQL() throws Exception {
        MySQLInfoSchemaResultHandler schemaResultHandler =
                new MySQLInfoSchemaResultHandler(this);
        ctx.executeNativeSQLParallJob(dataNodes,this.execSql,schemaResultHandler);
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
        server.getBusinessExecutor().execute(task);

        return true;
    }

    private class Task implements Runnable{
        @Override
        public void run() {

            if (!running.compareAndSet(false,true)) {
                return;
            }

            try {
                for (; ;) {
                    final PackWraper pack = packs.poll();

                    if(pack == null){
                        continue;
                    }

                    if (pack == END_FLAG_PACK) {

                        if (sqlQueryResultListener != null)
                            sqlQueryResultListener.onResult(mapHostData);

                        break;
                    }

                    mapHostData.get(pack.dataNode).add(pack.rowData);
                }
            } catch (final Exception e) {
                mapHostData = null;
                e.printStackTrace();
            } finally {
                running.set(false);
            }
        }
    }
}
