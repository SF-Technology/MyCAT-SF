package org.opencloudb.manager.parser.druid.statement;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;

/**
 * drop rule 保存语句的解析结果
 * @author 01140003
 * @version 2017年2月23日 下午5:38:47 
 */
public class MycatDropRuleStatement extends MycatStatementImpl implements SQLDDLStatement { 
	private SQLName rule;

	public SQLName getRule() {
		return rule;
	}

	public void setRule(SQLName rule) {
		this.rule = rule;
	}
}
