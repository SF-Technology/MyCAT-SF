package org.opencloudb.manager.parser.druid.statement;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;

public class MycatCreateRuleStatement extends MycatStatementImpl implements SQLDDLStatement{
	private SQLName rule;
	private SQLName column;
	private SQLName function;
	
	public SQLName getRule() {
		return rule;
	}
	public void setRule(SQLName rule) {
		this.rule = rule;
	}
	public SQLName getColumn() {
		return column;
	}
	public void setColumn(SQLName column) {
		this.column = column;
	}
	public SQLName getFunction() {
		return function;
	}
	public void setFunction(SQLName function) {
		this.function = function;
	}
	
	
}
