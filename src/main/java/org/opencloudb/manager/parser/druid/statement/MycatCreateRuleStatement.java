package org.opencloudb.manager.parser.druid.statement;

import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;

public class MycatCreateRuleStatement extends MycatStatementImpl implements SQLDDLStatement{
	private String  rule;
	private String column;
	private String function;
	
	public String getRule() {
		return rule;
	}
	public void setRule(String rule) {
		this.rule = rule;
	}
	public String getColumn() {
		return column;
	}
	public void setColumn(String column) {
		this.column = column;
	}
	public String getFunction() {
		return function;
	}
	public void setFunction(String function) {
		this.function = function;
	}
	
	
}
