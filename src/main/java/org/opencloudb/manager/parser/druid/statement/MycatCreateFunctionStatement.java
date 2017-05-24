package org.opencloudb.manager.parser.druid.statement;

import java.util.HashMap;
import java.util.Map;

import org.opencloudb.manager.parser.druid.MycatASTVisitor;
import org.opencloudb.manager.response.ListFunctions;
import org.opencloudb.route.function.AbstractPartitionAlgorithm;

import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;

/**
 * create function 语句解析结果
 * @author 01140003
 * @version 2017年2月23日 下午7:51:32 
 */
public class MycatCreateFunctionStatement extends MycatStatementImpl implements SQLDDLStatement{
	private String function;
	private String className;
	private Map<String, String> properties;
	
	
	@Override
    public void accept0(MycatASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    public static MycatCreateFunctionStatement from(String funcName, AbstractPartitionAlgorithm function) {
	    MycatCreateFunctionStatement stmt = new MycatCreateFunctionStatement();
	    stmt.setFunction(funcName);
	    stmt.setClassName(function.getClass().getName());
	    stmt.setProperties(getProperties(function));
	    return stmt;
	}
	
	private static Map<String, String> getProperties(AbstractPartitionAlgorithm function) {
	    Map<String, Object> prop = ListFunctions.acquireProperties(function);
	    Map<String, String> prop1 = new HashMap<String, String>(prop.size());
	    for (String key : prop.keySet()) {
	        String value = prop.get(key).toString();
	        prop1.put(key, value);
	    }
	    return prop1;
	}
	
	public String getFunction() {
		return function;
	}
	public void setFunction(String function) {
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
