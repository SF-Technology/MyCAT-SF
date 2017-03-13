package org.opencloudb.manager.parser.druid.statement;

import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;

public class MycatListVariablesStatement extends MycatListStatement implements SQLDDLStatement{
	private String matchExpr;

	public String getMatchExpr() {
		return matchExpr;
	}

	public void setMatchExpr(String matchExpr) {
		this.matchExpr = matchExpr;
	}
}
