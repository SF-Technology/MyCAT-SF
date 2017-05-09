package org.opencloudb.config.model;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * 防火墙配置
 * @author CrazyPig
 * @since 2017-02-22
 *
 */
public class FirewallConfig {
	
	/**
	 * SQL 防火墙功能配置选项 from druid 项目
	 */

	/**
	 *  SQL 防火墙开关 1开启，2 关闭，并记录拦截信息
	 */
	private int enableSQLFirewall;


	/**
	 * 是否启用正则表达式匹配SQL
	 */
	private boolean enableRegEx;


	/**
	 * 允许结果集，超过了maxAllowResultRow 动态添加到SQL黑名单中
	 */
	private int maxAllowResultRow;

	/**
	 *  maxAllowExecuteUnitTime s 内最大允许执行次数，超过了动态添加到SQL黑名单中
	 */
	private int maxAllowExecuteTimes;

	/**
	 * 单位为s,一条sql执行的时间，超过了, 则动态加入SQL黑名单中
	 */
	private int maxAllowExecuteSqlTime;
	
	/**
	 *  单位为s,一条sql执行的时间大于maxAllowExecuteSqlTime，并超过了多少countInMaxAllowExecuteSqlTime次数则加入动态加入SQL黑名单中
	 */
	public int countInMaxAllowExecuteSqlTime;

	/**
	 * 单位为s 默认配置1s 与maxAllowExecuteTimes配合使用
	 */
	private int maxAllowExecuteUnitTime;

	/**
	 * 拦截配置－语句
	 */
	private boolean selelctAllow;	//true	是否允许执行SELECT语句
	private boolean selectAllColumnAllow;	//true	是否允许执行SELECT * FROM T这样的语句。如果设置为false，不允许执行select * from t，但select * from (select id, name from t) a。这个选项是防御程序通过调用select *获得数据表的结构信息。
	private boolean selectIntoAllow;	//true	SELECT查询中是否允许INTO字句
	private boolean deleteAllow;	//true	是否允许执行DELETE语句
	private boolean updateAllow;	//true	是否允许执行UPDATE语句
	private boolean insertAllow;	//true	是否允许执行INSERT语句
	private boolean replaceAllow;	//true	是否允许执行REPLACE语句
	private boolean mergeAllow;	//true	是否允许执行MERGE语句，这个只在Oracle中有用
	private boolean callAllow;	//true	是否允许通过jdbc的call语法调用存储过程
	private boolean setAllow;	//true	是否允许使用SET语法
	private boolean truncateAllow;	//true	truncate语句是危险，缺省打开，若需要自行关闭
	private boolean createTableAllow;	//true	是否允许创建表
	private boolean alterTableAllow;	//true	是否允许执行Alter Table语句
	private boolean dropTableAllow;	//true	是否允许修改表
	private boolean commentAllow;	//false	是否允许语句中存在注释，Oracle的用户不用担心，Wall能够识别hints和注释的区别
	private boolean noneBaseStatementAllow;//false	是否允许非以上基本语句的其他语句，缺省关闭，通过这个选项就能够屏蔽DDL。
	private boolean multiStatementAllow;	//false	是否允许一次执行多条语句，缺省关闭
	private boolean useAllow;	//true	是否允许执行mysql的use语句，缺省打开
	private boolean describeAllow;	//true	是否允许执行mysql的describe语句，缺省打开
	private boolean showAllow;	//true	是否允许执行mysql的show语句，缺省打开
	private boolean commitAllow;	//true	是否允许执行commit操作
	private boolean rollbackAllow;	//true	是否允许执行roll back操作

	/**
	 * 拦截配置－永真条件
	 */
	private boolean selectWhereAlwayTrueCheck;	// true 检查SELECT语句的WHERE子句是否是一个永真条件
	private boolean selectHavingAlwayTrueCheck;	// true 检查SELECT语句的HAVING子句是否是一个永真条件
	private boolean deleteWhereAlwayTrueCheck;	// true 检查DELETE语句的WHERE子句是否是一个永真条件
	private boolean deleteWhereNoneCheck;	    // false 检查DELETE语句是否无where条件，这是有风险的，但不是SQL注入类型的风险
	private boolean updateWhereAlayTrueCheck;	// true 检查UPDATE语句的WHERE子句是否是一个永真条件
	private boolean updateWhereNoneCheck;	    // false 检查UPDATE语句是否无where条件，这是有风险的，但不是SQL注入类型的风险
	private boolean conditionAndAlwayTrueAllow;	// false检查查询条件(WHERE/HAVING子句)中是否包含AND永真条件
	private boolean conditionAndAlwayFalseAllow;	// false 检查查询条件(WHERE/HAVING子句)中是否包含AND永假条件
	private boolean conditionLikeTrueAllow;	    // true 检查查询条件(WHERE/HAVING子句)中是否包含LIKE永真条件


	/**
	 * 其他拦截配置
	 */
	private boolean selectIntoOutfileAllow;		//false SELECT ... INTO OUTFILE 是否允许，这个是mysql注入攻击的常见手段，缺省是禁止的
	private boolean selectUnionCheck;	       //true 检测SELECT UNION
	private boolean selectMinusCheck;	       //true 检测SELECT MINUS
	private boolean selectExceptChec;		   //true 检测SELECT EXCEPT
	private boolean selectIntersectCheck;	   //true 检测SELECT INTERSECT
	private boolean mustParameterized;		   //false 是否必须参数化，如果为True，则不允许类似WHERE ID = 1这种不参数化的SQL
	private boolean strictSyntaxCheck;		   //true 是否进行严格的语法检测，Druid SQL Parser在某些场景不能覆盖所有的SQL语法，出现解析SQL出错，可以临时把这个选项设置为false，同时把SQL反馈给Druid的开发者。
	private boolean conditionOpXorAllow;		   //false 查询条件中是否允许有XOR条件。XOR不常用，很难判断永真或者永假，缺省不允许。
	private boolean conditionOpBitwseAllow;		//true 查询条件中是否允许有"&"、"~"、"|"、"^"运算符。
	private boolean conditionDoubleConstAllow;		//false 查询条件中是否允许连续两个常量运算表达式
    private boolean minusAllow;		//true 是否允许SELECT * FROM A MINUS SELECT * FROM B这样的语句
	private boolean intersectAllow;		//true 是否允许SELECT * FROM A INTERSECT SELECT * FROM B这样的语句
	private boolean constArithmeticAllow; //true 拦截常量运算的条件，比如说WHERE FID = 3 - 1，其中"3 - 1"是常量运算表达式。
	private boolean limitZeroAllow;       	//false 是否允许limit 0这样的语句
	
	public FirewallConfig() {
		/**
		 * SQL 防火墙配置默认配置
		 */
		this.enableSQLFirewall = -1;
		this.maxAllowResultRow = 1000000;
		this.maxAllowExecuteTimes = 2000000;
		this.maxAllowExecuteSqlTime = 20000;
		this.countInMaxAllowExecuteSqlTime = 100000;
		this.maxAllowExecuteUnitTime = 2;
		this.enableRegEx = false;

		this.selelctAllow=true;
		this.selectAllColumnAllow=true;
		this.selectIntoAllow=true;
		this.deleteAllow=true;
		this.updateAllow=true;
		this.insertAllow=true;
		this.callAllow=true;
		this.setAllow=true;
		this.truncateAllow=true;
		this.createTableAllow=true;
		this.alterTableAllow=true;
		this.dropTableAllow=true;
		this.commentAllow=false;
		this.multiStatementAllow=false;
		this.useAllow=true;
		this.describeAllow=true;
		this.showAllow=true;
		this.commitAllow=true;
		this.rollbackAllow=true;

        /**
         * 拦截配置－永真条件
         */
        this.selectWhereAlwayTrueCheck = true;
        this.selectHavingAlwayTrueCheck = true;
        this.deleteWhereAlwayTrueCheck = true;
        this.deleteWhereNoneCheck = false;
        this.updateWhereAlayTrueCheck = true;
        this.updateWhereNoneCheck = false;
        this.conditionAndAlwayTrueAllow = false;
        this.conditionAndAlwayFalseAllow = false;
        this.conditionLikeTrueAllow = true;

        /**
         * 其他拦截配置
         */
        this.selectIntoOutfileAllow = false;
        this.selectUnionCheck = true;
        this.selectMinusCheck = true ;
        this.selectExceptChec = true ;
        this.selectIntersectCheck = true ;
        this.mustParameterized = false;
        this.strictSyntaxCheck = true ;
        this.conditionOpXorAllow = false ;
        this.conditionOpBitwseAllow = true ;
        this.conditionDoubleConstAllow = false ;
        this.minusAllow = true;
        this.intersectAllow = true;
        this.constArithmeticAllow = true ;
        this.limitZeroAllow = false;
	}
	
	/**
	 * 获得sqlwall变量的当前值
	 * @return
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 * @throws InvocationTargetException
	 * @throws IntrospectionException
	 */
	public Map<String, Object> currentSqlwallVariables() throws IllegalAccessException, 
	IllegalArgumentException, InvocationTargetException, IntrospectionException {
		return acquireVariables(this);
	}
	
	/**
	 * 获得可动态配置的属性
	 * @return
	 * @throws NoSuchFieldException
	 * @throws SecurityException
	 */
	public Set<String> dynamicVariables(){
		TreeSet<String> fields = new TreeSet<String>();
		
		fields.add("enableSQLFirewall");
		fields.add("enableRegEx");
		fields.add("maxAllowResultRow");
		fields.add("maxAllowExecuteTimes");
		fields.add("maxAllowExecuteSqlTime");
		fields.add("maxAllowExecuteUnitTime");
		
		return fields;
	}
	
	/**
	 * 获得sqlwall变量的默认值(new一个FirewallConfig实例，然后将配置信息映射到该实例中，然后从中取出默认配置信息)
	 * @return
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 * @throws InvocationTargetException
	 * @throws IntrospectionException
	 */
	public Map<String, Object> defaultSqlwallVariables() throws IllegalAccessException, 
	IllegalArgumentException, InvocationTargetException, IntrospectionException{
		
		FirewallConfig firewallConfig = new FirewallConfig();
		
		return acquireVariables(firewallConfig);
	}
	
	/**
	 * 通过反射，获得object对象中所有具有get和set方法的属性，以及这些属性的值
	 * @param object
	 * @return
	 * @throws IntrospectionException
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 * @throws InvocationTargetException
	 */
	private Map<String, Object> acquireVariables(Object object) throws IntrospectionException, 
	IllegalAccessException, IllegalArgumentException, InvocationTargetException{
		Map<String, Object> systemVariables = new TreeMap<String, Object>(); // key=属性，value=默认值
		
		BeanInfo beanInfo = Introspector.getBeanInfo(object.getClass());
		PropertyDescriptor[] pds = beanInfo.getPropertyDescriptors();
		
		for (PropertyDescriptor descriptor : pds) {
			Method getMethod = descriptor.getReadMethod();
			Method setMethod = descriptor.getWriteMethod();
			
			if (getMethod != null && setMethod != null) {
				Object value = getMethod.invoke(object);
				systemVariables.put(descriptor.getName(), value);
			}
		}
		
		return systemVariables;
	}
	
	public int getEnableSQLFirewall() {
		return enableSQLFirewall;
	}
	public void setEnableSQLFirewall(int enableSQLFirewall) {
		this.enableSQLFirewall = enableSQLFirewall;
	}
	public boolean isEnableRegEx() {
		return enableRegEx;
	}
	public void setEnableRegEx(boolean enableRegEx) {
		this.enableRegEx = enableRegEx;
	}
	public int getMaxAllowResultRow() {
		return maxAllowResultRow;
	}
	public void setMaxAllowResultRow(int maxAllowResultRow) {
		this.maxAllowResultRow = maxAllowResultRow;
	}
	public int getMaxAllowExecuteTimes() {
		return maxAllowExecuteTimes;
	}
	public void setMaxAllowExecuteTimes(int maxAllowExecuteTimes) {
		this.maxAllowExecuteTimes = maxAllowExecuteTimes;
	}
	public int getMaxAllowExecuteSqlTime() {
		return maxAllowExecuteSqlTime;
	}
	public void setMaxAllowExecuteSqlTime(int maxAllowExecuteSqlTime) {
		this.maxAllowExecuteSqlTime = maxAllowExecuteSqlTime;
	}
	public int getMaxAllowExecuteUnitTime() {
		return maxAllowExecuteUnitTime;
	}
	public void setMaxAllowExecuteUnitTime(int maxAllowExecuteUnitTime) {
		this.maxAllowExecuteUnitTime = maxAllowExecuteUnitTime;
	}
	public boolean isSelelctAllow() {
		return selelctAllow;
	}
	public void setSelelctAllow(boolean selelctAllow) {
		this.selelctAllow = selelctAllow;
	}
	public boolean isSelectAllColumnAllow() {
		return selectAllColumnAllow;
	}
	public void setSelectAllColumnAllow(boolean selectAllColumnAllow) {
		this.selectAllColumnAllow = selectAllColumnAllow;
	}
	public boolean isSelectIntoAllow() {
		return selectIntoAllow;
	}
	public void setSelectIntoAllow(boolean selectIntoAllow) {
		this.selectIntoAllow = selectIntoAllow;
	}
	public boolean isDeleteAllow() {
		return deleteAllow;
	}
	public void setDeleteAllow(boolean deleteAllow) {
		this.deleteAllow = deleteAllow;
	}
	public boolean isUpdateAllow() {
		return updateAllow;
	}
	public void setUpdateAllow(boolean updateAllow) {
		this.updateAllow = updateAllow;
	}
	public boolean isInsertAllow() {
		return insertAllow;
	}
	public void setInsertAllow(boolean insertAllow) {
		this.insertAllow = insertAllow;
	}
	public boolean isReplaceAllow() {
		return replaceAllow;
	}
	public void setReplaceAllow(boolean replaceAllow) {
		this.replaceAllow = replaceAllow;
	}
	public boolean isMergeAllow() {
		return mergeAllow;
	}
	public void setMergeAllow(boolean mergeAllow) {
		this.mergeAllow = mergeAllow;
	}
	public boolean isCallAllow() {
		return callAllow;
	}
	public void setCallAllow(boolean callAllow) {
		this.callAllow = callAllow;
	}
	public boolean isSetAllow() {
		return setAllow;
	}
	public void setSetAllow(boolean setAllow) {
		this.setAllow = setAllow;
	}
	public boolean isTruncateAllow() {
		return truncateAllow;
	}
	public void setTruncateAllow(boolean truncateAllow) {
		this.truncateAllow = truncateAllow;
	}
	public boolean isCreateTableAllow() {
		return createTableAllow;
	}
	public void setCreateTableAllow(boolean createTableAllow) {
		this.createTableAllow = createTableAllow;
	}
	public boolean isAlterTableAllow() {
		return alterTableAllow;
	}
	public void setAlterTableAllow(boolean alterTableAllow) {
		this.alterTableAllow = alterTableAllow;
	}
	public boolean isDropTableAllow() {
		return dropTableAllow;
	}
	public void setDropTableAllow(boolean dropTableAllow) {
		this.dropTableAllow = dropTableAllow;
	}
	public boolean isCommentAllow() {
		return commentAllow;
	}
	public void setCommentAllow(boolean commentAllow) {
		this.commentAllow = commentAllow;
	}
	public boolean isNoneBaseStatementAllow() {
		return noneBaseStatementAllow;
	}
	public void setNoneBaseStatementAllow(boolean noneBaseStatementAllow) {
		this.noneBaseStatementAllow = noneBaseStatementAllow;
	}
	public boolean isMultiStatementAllow() {
		return multiStatementAllow;
	}
	public void setMultiStatementAllow(boolean multiStatementAllow) {
		this.multiStatementAllow = multiStatementAllow;
	}
	public boolean isUseAllow() {
		return useAllow;
	}
	public void setUseAllow(boolean useAllow) {
		this.useAllow = useAllow;
	}
	public boolean isDescribeAllow() {
		return describeAllow;
	}
	public void setDescribeAllow(boolean describeAllow) {
		this.describeAllow = describeAllow;
	}
	public boolean isShowAllow() {
		return showAllow;
	}
	public void setShowAllow(boolean showAllow) {
		this.showAllow = showAllow;
	}
	public boolean isCommitAllow() {
		return commitAllow;
	}
	public void setCommitAllow(boolean commitAllow) {
		this.commitAllow = commitAllow;
	}
	public boolean isRollbackAllow() {
		return rollbackAllow;
	}
	public void setRollbackAllow(boolean rollbackAllow) {
		this.rollbackAllow = rollbackAllow;
	}
	public boolean isSelectWhereAlwayTrueCheck() {
		return selectWhereAlwayTrueCheck;
	}
	public void setSelectWhereAlwayTrueCheck(boolean selectWhereAlwayTrueCheck) {
		this.selectWhereAlwayTrueCheck = selectWhereAlwayTrueCheck;
	}
	public boolean isSelectHavingAlwayTrueCheck() {
		return selectHavingAlwayTrueCheck;
	}
	public void setSelectHavingAlwayTrueCheck(boolean selectHavingAlwayTrueCheck) {
		this.selectHavingAlwayTrueCheck = selectHavingAlwayTrueCheck;
	}
	public boolean isDeleteWhereAlwayTrueCheck() {
		return deleteWhereAlwayTrueCheck;
	}
	public void setDeleteWhereAlwayTrueCheck(boolean deleteWhereAlwayTrueCheck) {
		this.deleteWhereAlwayTrueCheck = deleteWhereAlwayTrueCheck;
	}
	public boolean isDeleteWhereNoneCheck() {
		return deleteWhereNoneCheck;
	}
	public void setDeleteWhereNoneCheck(boolean deleteWhereNoneCheck) {
		this.deleteWhereNoneCheck = deleteWhereNoneCheck;
	}
	public boolean isUpdateWhereAlayTrueCheck() {
		return updateWhereAlayTrueCheck;
	}
	public void setUpdateWhereAlayTrueCheck(boolean updateWhereAlayTrueCheck) {
		this.updateWhereAlayTrueCheck = updateWhereAlayTrueCheck;
	}
	public boolean isUpdateWhereNoneCheck() {
		return updateWhereNoneCheck;
	}
	public void setUpdateWhereNoneCheck(boolean updateWhereNoneCheck) {
		this.updateWhereNoneCheck = updateWhereNoneCheck;
	}
	public boolean isConditionAndAlwayTrueAllow() {
		return conditionAndAlwayTrueAllow;
	}
	public void setConditionAndAlwayTrueAllow(boolean conditionAndAlwayTrueAllow) {
		this.conditionAndAlwayTrueAllow = conditionAndAlwayTrueAllow;
	}
	public boolean isConditionAndAlwayFalseAllow() {
		return conditionAndAlwayFalseAllow;
	}
	public void setConditionAndAlwayFalseAllow(boolean conditionAndAlwayFalseAllow) {
		this.conditionAndAlwayFalseAllow = conditionAndAlwayFalseAllow;
	}
	public boolean isConditionLikeTrueAllow() {
		return conditionLikeTrueAllow;
	}
	public void setConditionLikeTrueAllow(boolean conditionLikeTrueAllow) {
		this.conditionLikeTrueAllow = conditionLikeTrueAllow;
	}
	public boolean isSelectIntoOutfileAllow() {
		return selectIntoOutfileAllow;
	}
	public void setSelectIntoOutfileAllow(boolean selectIntoOutfileAllow) {
		this.selectIntoOutfileAllow = selectIntoOutfileAllow;
	}
	public boolean isSelectUnionCheck() {
		return selectUnionCheck;
	}
	public void setSelectUnionCheck(boolean selectUnionCheck) {
		this.selectUnionCheck = selectUnionCheck;
	}
	public boolean isSelectMinusCheck() {
		return selectMinusCheck;
	}
	public void setSelectMinusCheck(boolean selectMinusCheck) {
		this.selectMinusCheck = selectMinusCheck;
	}
	public boolean isSelectExceptChec() {
		return selectExceptChec;
	}
	public void setSelectExceptChec(boolean selectExceptChec) {
		this.selectExceptChec = selectExceptChec;
	}
	public boolean isSelectIntersectCheck() {
		return selectIntersectCheck;
	}
	public void setSelectIntersectCheck(boolean selectIntersectCheck) {
		this.selectIntersectCheck = selectIntersectCheck;
	}
	public boolean isMustParameterized() {
		return mustParameterized;
	}
	public void setMustParameterized(boolean mustParameterized) {
		this.mustParameterized = mustParameterized;
	}
	public boolean isStrictSyntaxCheck() {
		return strictSyntaxCheck;
	}
	public void setStrictSyntaxCheck(boolean strictSyntaxCheck) {
		this.strictSyntaxCheck = strictSyntaxCheck;
	}
	public boolean isConditionOpXorAllow() {
		return conditionOpXorAllow;
	}
	public void setConditionOpXorAllow(boolean conditionOpXorAllow) {
		this.conditionOpXorAllow = conditionOpXorAllow;
	}
	public boolean isConditionOpBitwseAllow() {
		return conditionOpBitwseAllow;
	}
	public void setConditionOpBitwseAllow(boolean conditionOpBitwseAllow) {
		this.conditionOpBitwseAllow = conditionOpBitwseAllow;
	}
	public boolean isConditionDoubleConstAllow() {
		return conditionDoubleConstAllow;
	}
	public void setConditionDoubleConstAllow(boolean conditionDoubleConstAllow) {
		this.conditionDoubleConstAllow = conditionDoubleConstAllow;
	}
	public boolean isMinusAllow() {
		return minusAllow;
	}
	public void setMinusAllow(boolean minusAllow) {
		this.minusAllow = minusAllow;
	}
	public boolean isIntersectAllow() {
		return intersectAllow;
	}
	public void setIntersectAllow(boolean intersectAllow) {
		this.intersectAllow = intersectAllow;
	}
	public boolean isConstArithmeticAllow() {
		return constArithmeticAllow;
	}
	public void setConstArithmeticAllow(boolean constArithmeticAllow) {
		this.constArithmeticAllow = constArithmeticAllow;
	}
	public boolean isLimitZeroAllow() {
		return limitZeroAllow;
	}
	public void setLimitZeroAllow(boolean limitZeroAllow) {
		this.limitZeroAllow = limitZeroAllow;
	}
	public int getCountInMaxAllowExecuteSqlTime() {
		return countInMaxAllowExecuteSqlTime;
	}
	public void setCountInMaxAllowExecuteSqlTime(int countInMaxAllowExecuteSqlTime) {
		this.countInMaxAllowExecuteSqlTime = countInMaxAllowExecuteSqlTime;
	}
	
}
