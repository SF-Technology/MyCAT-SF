package org.opencloudb.manager.parser.druid.statement;

import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;

public class MycatShowMapFileStatement extends MycatStatementImpl implements SQLDDLStatement {
	String fileName;

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
}
