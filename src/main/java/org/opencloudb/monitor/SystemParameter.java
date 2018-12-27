package org.opencloudb.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author zagnix
 * @create 2016-11-04 9:50
 */

public class SystemParameter {

    private final static Logger LOGGER =
            LoggerFactory.getLogger(ThreadPoolInfo.class);

    private String varName;
    private String varValue;
    private String describe;

    public final static String[][] SYS_PARAM = {
            /**name ,value, desc*/
            {"serverPort", "Mycat服务端口"},
            {"managerPort", "Mycat管理端口"},
            {"charset", "Mycat字符集"},
            {"processors", "处理器数目"},
            {"processorExecutor", "线程池大小"},
            {"processorBufferPool", "内存buffer pool 数目"},
            {"processorBufferChunk", "每块buffer chunk size"},
            {"txIsolation", "事务隔离级别"},
            {"useOffHeapForMerge", "是否使用off Heap方式处理结果集汇聚,1为启用"},
            {"memoryPageSize", "off Heap方式中内存页的大小"},
            {"spillsFileBufferSize", "写数据到磁盘临时buffer大小"},
            {"useStreamOutput", "是否启用流式输出 1启用"},
            {"useSqlStat", "是否启用sql stat功能 1启用，0不启用"},
            {"systemReserveMemorySize", "系统预留内存，在off-heap方式下，该参数无用"},
            {"dataNodeSortedTempDir", "off-heap内存模式下，内存不足时，数据写入磁盘的目录"},
            {"enableSqlStat", "1.启用采集全部sql信息入H2DB库. 0.不启用"},
            {"monitorUpdatePeriod", " 后台线程定时采集信息入H2DB库 单位ms"},
            {"sqlInMemDBPeriod", "SQL执行的情况在内存数据库中停留时间.. 单位ms"},
            {"bySqlTypeSummaryPeriod", "间隔根据SQL类型汇总，SQL执行次数.. 单位ms"},
            {"topNSummaryPeriod", "间隔取执行结果集和SQL执行时间TOP N.. 单位ms"},
            {"topExecuteResultN", "SQL执行结果集 TOP N"},
            {"topSqlExecuteTimeN", "SQL执行时间 TOP N"},
            {"limitConcurrentQuery", "限制并发查询度，控制cpu的消耗"},
            {"processorsRatio", "大sql查询时,配置processor使用比例"}
    };


    public final static String[][] SQLFIREWALL_PARAM = {
            /**
             * SQL 防火墙配置默认配置
            */
            {"enableSQLFirewall", "SQL防火墙开关 -1 关闭防火墙，关闭拦截, 0 开启防火墙 关闭拦截 打印log警告信息，1 开启防火墙，打开拦截 在client端提示信息，并记录监控表里 ,2.开启防火墙，关闭拦截 拦截信息记录到录监控表里，不打印log日志"},
            {"maxAllowResultRow", "结果集，超过了maxAllowResultRow 动态添加到SQL黑名单中"},
            {"maxAllowExecuteTimes", "maxAllowExecuteUnitTime s 内最大允许执行次数，超过了动态添加到SQL黑名单中"},
            {"maxAllowExecuteSqlTime", "单位为ms,一条sql执行的时间，超过了, 则动态加入SQL黑名单中"},
            {"countInMaxAllowExecuteSqlTime", "单位为s,一条sql执行的时间大于maxAllowExecuteSqlTime，并超过了多少countInMaxAllowExecuteSqlTime次数则加入动态加入SQL黑名单中"},
            {"maxAllowExecuteUnitTime", "单位为s 与 maxAllowExecuteTimes 配合使用"}

    };

    public final static String[][] SQLFIREWALL_PARAM_BOOL = {
            /**
             * SQL 防火墙配置默认配置
            */
            {"selelctAllow", "是否允许执行SELECT语句"},
            {"selectAllColumnAllow", "是否允许执行SELECT * FROM T这样的语句。如果设置为false，不允许执行select * from t，但select * from (select id\", name from t) a 这个选项是防御程序通过调用select *获得数据表的结构信息"},
            {"selectIntoAllow", "SELECT查询中是否允许INTO字句"},
            {"deleteAllow", "是否允许执行DELETE语句"},
            {"updateAllow", "是否允许执行UPDATE语句"},
            {"insertAllow", "是否允许执行INSERT语句"},
            {"callAllow", "是否允许通过jdbc的call语法调用存储过程"},
            {"setAllow", "是否允许使用SET语法"},
            {"truncateAllow", "truncate语句是危险，缺省打开，若需要自行关闭"},
            {"createTableAllow", "是否允许创建表"},
            {"alterTableAllow", "是否允许执行Alter Table语句"},
            {"dropTableAllow", "	是否允许修改表"},
            {"commentAllow", "	是否允许语句中存在注释，Oracle的用户不用担心，Wall能够识别hints和注释的区别"},
            {"multiStatementAllow", "是否允许一次执行多条语句，缺省关闭"},
            {"useAllow", "	是否允许执行mysql的use语句，缺省打开"},
            {"describeAllow", "是否允许执行mysql的describe语句，缺省打开"},
            {"showAllow", "是否允许执行mysql的show语句，缺省打开"},
            {"commitAllow", "是否允许执行commit操作"},
            {"rollbackAllow", "是否允许执行roll back操作"},
            {"selectWhereAlwayTrueCheck", "检查SELECT语句的WHERE子句是否是一个永真条件"},
            {"selectHavingAlwayTrueCheck", "检查SELECT语句的HAVING子句是否是一个永真条件"},
            {"deleteWhereAlwayTrueCheck", "检查DELETE语句的WHERE子句是否是一个永真条件"},
            {"deleteWhereNoneCheck", "检查DELETE语句是否无where条件，这是有风险的，但不是SQL注入类型的风险"},
            {"updateWhereAlayTrueCheck", "检查UPDATE语句的WHERE子句是否是一个永真条件"},
            {"updateWhereNoneCheck", " 检查UPDATE语句是否无where条件，这是有风险的，但不是SQL注入类型的风险"},
            {"conditionAndAlwayTrueAllow", "检查查询条件(WHERE/HAVING子句)中是否包含AND永真条件"},
            {"conditionAndAlwayFalseAllow", "检查查询条件(WHERE/HAVING子句)中是否包含AND永假条件"},
            {"conditionLikeTrueAllow", "检查查询条件(WHERE/HAVING子句)中是否包含LIKE永真条件"},

            {"selectIntoOutfileAllow", "SELECT ... INTO OUTFILE 是否允许，这个是mysql注入攻击的常见手段，缺省是禁止的"},
            {"selectUnionCheck", " 检测SELECT UNION"},
            {"selectMinusCheck", " 检测SELECT MINUS"},
            {"selectExceptChec", " 检测SELECT EXCEPT"},
            {"selectIntersectCheck", " 检测SELECT INTERSECT"},
            {"mustParameterized", " 是否必须参数化，如果为True，则不允许类似WHERE ID = 1这种不参数化的SQL"},
            {"strictSyntaxCheck", " 是否进行严格的语法检测，Druid SQL Parser在某些场景不能覆盖所有的SQL语法，出现解析SQL出错，可以临时把这个选项设置为false，同时把SQL反馈给Druid的开发者"},
            {"conditionOpXorAllow", " 查询条件中是否允许有XOR条件。XOR不常用，很难判断永真或者永假，缺省不允许"},
            {"conditionOpBitwseAllow", " 查询条件中是否允许有\" & \"、\"~\"、\" | \"、\" ^ \"运算符"},
            {"conditionDoubleConstAllow", " 查询条件中是否允许连续两个常量运算表达式"},
            {"minusAllow", " 是否允许SELECT * FROM A MINUS SELECT * FROM B这样的语句"},
            {"intersectAllow", " 是否允许SELECT * FROM A INTERSECT SELECT * FROM B这样的语句"},
            {"constArithmeticAllow", " 拦截常量运算的条件，比如说WHERE FID = 3 - 1，其中\"3 - 1\"是常量运算表达式"},
            {"limitZeroAllow", " 是否允许limit 0这样的语句"}
    };


    public String getVarName() {
        return varName;
    }

    public void setVarName(String varName) {
        this.varName = varName;
    }

    public String getVarValue() {
        return varValue;
    }

    public void setVarValue(String varValue) {
        this.varValue = varValue;
    }

    public String getDescribe() {
        return describe;
    }

    public void setDescribe(String describe) {
        this.describe = describe;
    }

    @Override
    public String toString() {
        return "SystemParameter{" +
                "varName='" + varName + '\'' +
                ", varValue='" + varValue + '\'' +
                ", describe='" + describe + '\'' +
                '}';
    }


    public void update() {

        /**
         * 1.查询是已经存在
         */
        boolean isAdd = true;
        final Connection h2DBConn =
                H2DBMonitorManager.getH2DBMonitorManager().getH2DBMonitorConn();
        Statement stmt = null;
        ResultSet rset = null;

        try {

            String sql = "select var_name from t_sysparam where var_name = '" + getVarName() + "'";
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("sql === >  " + sql);
            }
            stmt = h2DBConn.createStatement();
            rset = stmt.executeQuery(sql);
            if (rset.next()) {
                isAdd = false;
            }

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

        /**
         * 2.根据1决定是添加还是更新
         */
        String sql = null;

        if (isAdd) {
            sql = "INSERT INTO t_sysparam VALUES('" + getVarName() + "','" + getVarValue() + "','"
                    + getDescribe() + "')";
        } else {
            sql = "UPDATE t_memory SET var_value ='" + getVarValue() + "'," +
                    " WHERE var_name = " + getVarName();
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("sql === >  " + sql);
        }

        try {
            stmt = h2DBConn.createStatement();
            stmt.executeUpdate(sql);
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


}
