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

    public final  static String [][] SYS_PARAM={
            /**name ,value, desc*/
            {"serverPort"," "},
            {"managerPort"," "},
            {"charset"," "},
            {"processors"," "},
            {"processorExecutor"," "},
            {"processorBufferPool"," "},
            {"txIsolation"," "},
            {"useOffHeapForMerge"," "},
            {"memoryPageSize"," "},
            {"spillsFileBufferSize"," "},
            {"useStreamOutput"," "},
            {"useSqlStat"," "},
            {"systemReserveMemorySize"," "},
            {"dataNodeSortedTempDir"," "},

            /**
             * SQL 防火墙配置默认配置
            */
            {"enableSQLFirewall"," "},
            {"maxAllowResultRow"," "},
            {"maxAllowExecuteTimes"," "},
            {"maxAllowExecuteSqlTime"," "},
            {"maxAllowExecuteUnitTime"," "},
            {"monitorUpdatePeriod"," "},
            {"sqlInMemDBPeriod","SQL执行的情况在内存数据库中停留时间.. 单位ms"},
            {"bySqlTypeSummaryPeriod","间隔根据SQL类型汇总，SQL执行次数.. 单位ms"},
            {"topNSummaryPeriod","间隔取执行结果集和SQL执行时间TOP N.. 单位ms"},
            {"topExecuteResultN","SQL执行结果集 TOP N"},
            {"topSqlExecuteTimeN","SQL执行时间 TOP N"}
    };

    public final static String [][] SYS_PARAM_BOOL={
            /**
             * SQL 防火墙配置默认配置
             */
            {"enableRegEx"," "},
            {"selectAllColumnAllow"," "},
            {"selectWhereAlwayTrueCheck"," "},
            {"selectHavingAlwayTrueCheck"," "},
            {"deleteWhereAlwayTrueCheck"," "},
            {"deleteWhereNoneCheck"," "},
            {"updateWhereAlayTrueCheck"," "},
            {"updateWhereNoneCheck"," "},
            {"conditionAndAlwayTrueAllow"," "},
            {"conditionAndAlwayFalseAllow"," "},
            {"conditionLikeTrueAllow"," "},
            /**
             * 其他拦截配置
            */
            {"selectIntoOutfileAllow"," "},
            {"selectUnionCheck"," "},
            {"selectMinusCheck"," "},
            {"selectExceptChec"," "},
            {"selectIntersectCheck"," "},
            {"mustParameterized"," "},
            {"strictSyntaxCheck"," "},
            {"conditionOpXorAllow"," "},
            {"conditionOpBitwseAllow"," "},
            {"conditionDoubleConstAllow"," "},
            {"minusAllow"," "},
            {"intersectAllow"," "},
            {"constArithmeticAllow"," "},
            {"limitZeroAllow"," "}
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
            if (rset.next()){
                isAdd = false;
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

        /**
         * 2.根据1决定是添加还是更新
         */
        String sql = null;

        if(isAdd){
            sql = "INSERT INTO t_sysparam VALUES('" +getVarName()+ "','" + getVarValue() + "','"
                    + getDescribe() + "')";
        }else {
            sql = "UPDATE t_memory SET var_value ='" + getVarValue() +"'," +
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
    }



}
