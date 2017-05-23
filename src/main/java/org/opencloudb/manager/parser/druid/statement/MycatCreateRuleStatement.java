package org.opencloudb.manager.parser.druid.statement;

import org.opencloudb.config.model.rule.RuleConfig;
import org.opencloudb.manager.parser.druid.MycatASTVisitor;

import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;

public class MycatCreateRuleStatement extends MycatStatementImpl implements SQLDDLStatement{
	private String  rule;
	private String column;
	private String function;
	
	@Override
    public void accept0(MycatASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    public static MycatCreateRuleStatement from(RuleConfig ruleConf) {
	    MycatCreateRuleStatement stmt = new MycatCreateRuleStatement();
	    stmt.setRule(ruleConf.getName());
	    stmt.setColumn(ruleConf.getColumn());
	    stmt.setFunction(ruleConf.getFunctionName());
	    return stmt;
	}
	
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
