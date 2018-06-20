package org.opencloudb.manager.parser.druid.statement;

import org.opencloudb.manager.parser.druid.MycatASTVisitor;

import com.alibaba.druid.sql.ast.SQLName;

/**
 * 封装检查全局表数据一致性命令的语句
 * 
 * <pre>
 * sql format : <br>
 * checksum table schema.tablename;
 * </pre>
 * @author CrazyPig
 * @since 2017-01-13
 *
 */
public class MycatChecksumTableStatement extends MycatStatementImpl {
	
	private SQLName tableName;

	public SQLName getTableName() {
		return tableName;
	}

	public void setTableName(SQLName tableName) {
		this.tableName = tableName;
	}
	
	@Override
	public void accept0(MycatASTVisitor visitor) {
		visitor.visit(this);
		visitor.endVisit(this);
	}
	
}
