package org.opencloudb.manager.parser.druid.statement;

import java.util.List;

import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;

/**
 * create mapfile 语句的解析结果
 * @author 01140003
 * @version 2017年4月12日 上午10:49:49 
 */
public class MycatCreateMapFileStatement extends MycatStatementImpl implements SQLDDLStatement {
	private String fileName;
	private List<String> lines;
	
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public List<String> getLines() {
		return lines;
	}
	public void setLines(List<String> lines) {
		this.lines = lines;
	}
}
