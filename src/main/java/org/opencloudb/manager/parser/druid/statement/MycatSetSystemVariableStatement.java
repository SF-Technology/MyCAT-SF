package org.opencloudb.manager.parser.druid.statement;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;

public class MycatSetSystemVariableStatement extends MycatStatementImpl implements SQLDDLStatement{
	private SQLName variableName;
	private String variableValue;
	
	public SQLName getVariableName() {
		return variableName;
	}
	public void setVariableName(SQLName variableName) {
		this.variableName = variableName;
	}
	public String getVariableValue() {
		return variableValue;
	}
	public void setVariableValue(String variableValue) {
		this.variableValue = variableValue;
	}
	
	
}
