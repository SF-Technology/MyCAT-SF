package org.opencloudb.sqlfw;

import com.alibaba.druid.wall.WallConfig;
import com.alibaba.druid.wall.WallProvider;
import com.alibaba.druid.wall.spi.MySqlWallProvider;
import org.opencloudb.MycatServer;
import org.opencloudb.config.model.SystemConfig;

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
        SystemConfig systemConfig = MycatServer.getInstance().getConfig().getSystem();
        /**
         * 配置druid parser wall功能
         */


        /**
         * SQL 防火墙配置默认配置
         */

        //selelctAllow = true
        wallConfig.setSelelctAllow(systemConfig.isSelelctAllow());

        //selectAllColumnAllow = true;
        wallConfig.setSelectAllColumnAllow(systemConfig.isSelectAllColumnAllow());

        // selectIntoAllow = true;
        wallConfig.setSelectIntoAllow(systemConfig.isSelectIntoAllow());

        //deleteAllow = true;
        wallConfig.setDeleteAllow(systemConfig.isDeleteAllow());

        //updateAllow= true;
        wallConfig.setUpdateAllow(systemConfig.isUpdateAllow());

        //insertAllow = true;
        wallConfig.setInsertAllow(systemConfig.isInsertAllow());

        //callAllow = true;
        wallConfig.setCallAllow(systemConfig.isCallAllow());

        // setAllow = true;
        wallConfig.setSetAllow(systemConfig.isSetAllow());

        // truncateAllow = true
        wallConfig.setTruncateAllow(systemConfig.isTruncateAllow());

        // createTableAllow = true
        wallConfig.setCreateTableAllow(systemConfig.isCreateTableAllow());

        // alterTableAllow = true
        wallConfig.setAlterTableAllow(systemConfig.isAlterTableAllow());

        //dropTableAllow = true
        wallConfig.setDropTableAllow(systemConfig.isDropTableAllow());
        // commentAllow = true
        wallConfig.setCommentAllow(systemConfig.isCommentAllow());

        //noneBaseStatementAllow=true;
        wallConfig.setNoneBaseStatementAllow(true);

        //multiStatementAllow = false
        wallConfig.setMultiStatementAllow(systemConfig.isMultiStatementAllow());

        // useAllow = true
        wallConfig.setUseAllow(systemConfig.isUseAllow());

        //describeAllow = true
        wallConfig.setDescribeAllow(systemConfig.isDescribeAllow());

        //showAllow = true
        wallConfig.setShowAllow(systemConfig.isShowAllow());

        //commitAllow = true
        wallConfig.setCommitAllow(systemConfig.isCommitAllow());

        //rollbackAllow = true
        wallConfig.setRollbackAllow(systemConfig.isRollbackAllow());



        /**
         * 拦截配置－永真条件
         */
        //selectWhereAlwayTrueCheck = true;
        wallConfig.
                setSelectWhereAlwayTrueCheck(systemConfig.isSelectWhereAlwayTrueCheck());
        //selectHavingAlwayTrueCheck = true;
        wallConfig.
                setSelectHavingAlwayTrueCheck(systemConfig.isSelectHavingAlwayTrueCheck());

        //deleteWhereAlwayTrueCheck = true;
        wallConfig.
                setDeleteWhereAlwayTrueCheck(systemConfig.isDeleteWhereAlwayTrueCheck());

        //deleteWhereNoneCheck = false;
        wallConfig.
                setDeleteWhereNoneCheck(systemConfig.isDeleteWhereNoneCheck());

        //updateWhereAlayTrueCheck = true;
        wallConfig.
                setUpdateWhereAlayTrueCheck(systemConfig.isUpdateWhereAlayTrueCheck());
        //updateWhereNoneCheck = false;
        wallConfig.
                setUpdateWhereNoneCheck(systemConfig.isUpdateWhereNoneCheck());
        //conditionAndAlwayTrueAllow = false;
        wallConfig.
                setConditionAndAlwayTrueAllow(systemConfig.isConditionAndAlwayTrueAllow());
        //conditionAndAlwayFalseAllow = false;
        wallConfig.
                setConditionAndAlwayFalseAllow(systemConfig.isConditionAndAlwayFalseAllow());
        //conditionLikeTrueAllow = true;
        wallConfig.
                setConditionLikeTrueAllow(systemConfig.isConditionLikeTrueAllow());

        /**
         * 其他拦截配置
         */
        // selectIntoOutfileAllow = false;
        wallConfig.
                setSelectIntoOutfileAllow(systemConfig.isSelectIntoOutfileAllow());
        // selectUnionCheck = true;
        wallConfig.
                setSelectUnionCheck(systemConfig.isSelectUnionCheck());
        //selectMinusCheck = true ;
        wallConfig.
                setSelectMinusCheck(systemConfig.isSelectMinusCheck());
        //selectExceptChec = true ;
        wallConfig.
                setSelectExceptCheck(systemConfig.isSelectExceptChec());
        //selectIntersectCheck = true ;
        wallConfig.
                setSelectIntersectCheck(systemConfig.isSelectIntersectCheck());
        //this.mustParameterized = false;
        wallConfig.
                setMustParameterized(systemConfig.isMustParameterized());
        //strictSyntaxCheck = true ;
        wallConfig.
                setStrictSyntaxCheck(systemConfig.isStrictSyntaxCheck());
        //conditionOpXorAllow = false ;
        wallConfig.
                setConditionOpXorAllow(systemConfig.isConditionOpXorAllow());
        //conditionOpBitwseAllow = true ;
        wallConfig.
                setConditionOpBitwseAllow(systemConfig.isConditionOpBitwseAllow());
        //conditionDoubleConstAllow = false ;
        wallConfig.
                setConditionDoubleConstAllow(systemConfig.isConditionDoubleConstAllow());
        // minusAllow = true;
        wallConfig.
                setMinusAllow(systemConfig.isMinusAllow());
        //intersectAllow = true;
        wallConfig.
                setIntersectAllow(systemConfig.isIntersectAllow());
        //constArithmeticAllow = true ;
        wallConfig.
                setConstArithmeticAllow(systemConfig.isConstArithmeticAllow());
        //limitZeroAllow = false;
        wallConfig.
                setLimitZeroAllow(systemConfig.isLimitZeroAllow());



        provider = new MySqlWallProvider(wallConfig);

        if (systemConfig.enableSQLFirewall >= 0)
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
