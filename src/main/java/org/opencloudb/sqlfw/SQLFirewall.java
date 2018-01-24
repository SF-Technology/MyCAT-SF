package org.opencloudb.sqlfw;

import com.alibaba.druid.wall.WallConfig;
import com.alibaba.druid.wall.WallProvider;
import com.alibaba.druid.wall.spi.MySqlWallProvider;
import org.opencloudb.MycatServer;
import org.opencloudb.config.model.FirewallConfig;

/**
 * SQL 防火墙功能
 *
 * @author zagnix
 * @version 1.0
 * @create 2016-09-28 16:25
 */

public final  class SQLFirewall {

    private final WallConfig wallConfig = new WallConfig();
    private final WallProvider provider;
    private static final SQLFirewall sqlFirewall = new SQLFirewall();

    private SQLFirewall(){
        FirewallConfig firewallConf = MycatServer.getInstance().getConfig().getFirewall();
        /**
         * 配置druid parser wall功能
         */

        /**
         * SQL 防火墙配置默认配置
         */
        //selelctAllow = true
        wallConfig.setSelelctAllow(firewallConf.isSelelctAllow());

        //selectAllColumnAllow = true;
        wallConfig.setSelectAllColumnAllow(firewallConf.isSelectAllColumnAllow());

        // selectIntoAllow = true;
        wallConfig.setSelectIntoAllow(firewallConf.isSelectIntoAllow());

        //deleteAllow = true;
        wallConfig.setDeleteAllow(firewallConf.isDeleteAllow());

        //updateAllow= true;
        wallConfig.setUpdateAllow(firewallConf.isUpdateAllow());

        //insertAllow = true;
        wallConfig.setInsertAllow(firewallConf.isInsertAllow());

        //callAllow = true;
        wallConfig.setCallAllow(firewallConf.isCallAllow());

        // setAllow = true;
        wallConfig.setSetAllow(firewallConf.isSetAllow());

        // truncateAllow = true
        wallConfig.setTruncateAllow(firewallConf.isTruncateAllow());

        // createTableAllow = true
        wallConfig.setCreateTableAllow(firewallConf.isCreateTableAllow());

        // alterTableAllow = true
        wallConfig.setAlterTableAllow(firewallConf.isAlterTableAllow());

        //dropTableAllow = true
        wallConfig.setDropTableAllow(firewallConf.isDropTableAllow());
        // commentAllow = true
        wallConfig.setCommentAllow(firewallConf.isCommentAllow());

        //noneBaseStatementAllow=true;
        wallConfig.setNoneBaseStatementAllow(true);

        //multiStatementAllow = false
        wallConfig.setMultiStatementAllow(firewallConf.isMultiStatementAllow());

        // useAllow = true
        wallConfig.setUseAllow(firewallConf.isUseAllow());

        //describeAllow = true
        wallConfig.setDescribeAllow(firewallConf.isDescribeAllow());

        //showAllow = true
        wallConfig.setShowAllow(firewallConf.isShowAllow());

        //commitAllow = true
        wallConfig.setCommitAllow(firewallConf.isCommitAllow());

        //rollbackAllow = true
        wallConfig.setRollbackAllow(firewallConf.isRollbackAllow());



        /**
         * 拦截配置－永真条件
         */
        //selectWhereAlwayTrueCheck = true;
        wallConfig.
                setSelectWhereAlwayTrueCheck(firewallConf.isSelectWhereAlwayTrueCheck());
        //selectHavingAlwayTrueCheck = true;
        wallConfig.
                setSelectHavingAlwayTrueCheck(firewallConf.isSelectHavingAlwayTrueCheck());

        //deleteWhereAlwayTrueCheck = true;
        wallConfig.
                setDeleteWhereAlwayTrueCheck(firewallConf.isDeleteWhereAlwayTrueCheck());

        //deleteWhereNoneCheck = false;
        wallConfig.
                setDeleteWhereNoneCheck(firewallConf.isDeleteWhereNoneCheck());

        //updateWhereAlayTrueCheck = true;
        wallConfig.
                setUpdateWhereAlayTrueCheck(firewallConf.isUpdateWhereAlayTrueCheck());
        //updateWhereNoneCheck = false;
        wallConfig.
                setUpdateWhereNoneCheck(firewallConf.isUpdateWhereNoneCheck());
        //conditionAndAlwayTrueAllow = false;
        wallConfig.
                setConditionAndAlwayTrueAllow(firewallConf.isConditionAndAlwayTrueAllow());
        //conditionAndAlwayFalseAllow = false;
        wallConfig.
                setConditionAndAlwayFalseAllow(firewallConf.isConditionAndAlwayFalseAllow());
        //conditionLikeTrueAllow = true;
        wallConfig.
                setConditionLikeTrueAllow(firewallConf.isConditionLikeTrueAllow());

        /**
         * 其他拦截配置
         */
        // selectIntoOutfileAllow = false;
        wallConfig.
                setSelectIntoOutfileAllow(firewallConf.isSelectIntoOutfileAllow());
        // selectUnionCheck = true;
        wallConfig.
                setSelectUnionCheck(firewallConf.isSelectUnionCheck());
        //selectMinusCheck = true ;
        wallConfig.
                setSelectMinusCheck(firewallConf.isSelectMinusCheck());
        //selectExceptChec = true ;
        wallConfig.
                setSelectExceptCheck(firewallConf.isSelectExceptChec());
        //selectIntersectCheck = true ;
        wallConfig.
                setSelectIntersectCheck(firewallConf.isSelectIntersectCheck());
        //this.mustParameterized = false;
        wallConfig.
                setMustParameterized(firewallConf.isMustParameterized());
        //strictSyntaxCheck = true ;
        wallConfig.
                setStrictSyntaxCheck(firewallConf.isStrictSyntaxCheck());
        //conditionOpXorAllow = false ;
        wallConfig.
                setConditionOpXorAllow(firewallConf.isConditionOpXorAllow());
        //conditionOpBitwseAllow = true ;
        wallConfig.
                setConditionOpBitwseAllow(firewallConf.isConditionOpBitwseAllow());
        //conditionDoubleConstAllow = false ;
        wallConfig.
                setConditionDoubleConstAllow(firewallConf.isConditionDoubleConstAllow());
        // minusAllow = true;
        wallConfig.
                setMinusAllow(firewallConf.isMinusAllow());
        //intersectAllow = true;
        wallConfig.
                setIntersectAllow(firewallConf.isIntersectAllow());
        //constArithmeticAllow = true ;
        wallConfig.
                setConstArithmeticAllow(firewallConf.isConstArithmeticAllow());
        //limitZeroAllow = false;
        wallConfig.
                setLimitZeroAllow(firewallConf.isLimitZeroAllow());

        provider = new MySqlWallProvider(wallConfig);

        if (firewallConf.getEnableSQLFirewall() >= 0)
            provider.setBlackListEnable(true);
        else
            provider.setBlackListEnable(false);

    }

    public WallConfig getWallConfig() {
        return wallConfig;
    }

    public WallProvider getProvider() {
        return provider;
    }

    public static SQLFirewall getSqlFirewall() {
        return sqlFirewall;
    }

}