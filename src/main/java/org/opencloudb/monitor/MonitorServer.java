package org.opencloudb.monitor;



import org.opencloudb.MycatServer;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.jdbc.JDBCConnection;
import org.opencloudb.mysql.nio.MySQLConnection;
import org.opencloudb.net.BackendAIOConnection;
import org.opencloudb.net.NIOProcessor;
import org.opencloudb.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
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

    }

    private TimerTask doUpateMonitorInfo() {
        return new TimerTask() {
            @Override
            public void run() {
                updateMonitorInfoExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        updateMemoryInfo();
                    }
                });
            }
        };
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
