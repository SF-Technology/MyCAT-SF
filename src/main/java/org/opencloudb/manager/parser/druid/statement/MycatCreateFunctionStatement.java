package org.opencloudb.manager.parser.druid.statement;

import java.util.Map;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;

/**
 * create function 语句解析结果
 * @author 01140003
 * @version 2017年2月23日 下午7:51:32 
 */
public class MycatCreateFunctionStatement extends MycatStatementImpl implements SQLDDLStatement{
	private SQLName function;
	private String className;
	private Map<String, String> properties;
	
	public SQLName getFunction() {
		return function;
	}
	public void setFunction(SQLName function) {
		this.function = function;
	}
	public String getClassName() {
		return className;
	}
	public void setClassName(String className) {
		this.className = className;
	}
	public Map<String, String> getProperties() {
		return properties;
	}
	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}
	
}
