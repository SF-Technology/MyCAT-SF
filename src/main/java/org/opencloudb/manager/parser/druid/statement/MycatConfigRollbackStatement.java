package org.opencloudb.manager.parser.druid.statement;

import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;

/**
 * @author 01140003
 * @version 2017年5月18日 下午6:05:12 
 */
public class MycatConfigRollbackStatement extends MycatStatementImpl implements SQLDDLStatement{
	private int index; // 备份文件的序号

	public int getIndex() {
		return index;
	}

	public void setIndex(int id) {
		this.index = id;
	}
}
