package org.opencloudb.manager.parser.druid.statement;

import org.opencloudb.manager.parser.druid.MycatASTVisitor;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;

/**
 * 封装 create table 语句解析结果
 * @author CrazyPig
 * @since 2017-02-06
 *
 */
public class MycatCreateChildTableStatement extends MycatStatementImpl implements SQLDDLStatement {

	private SQLName table;
	private SQLName schema;
	private SQLExpr parentTable;
	private SQLExpr primaryKey;
	private SQLExpr parentKey;
	private SQLExpr joinKey;
	private boolean autoIncrement;
	
	@Override
	public void accept0(MycatASTVisitor visitor) {
		visitor.visit(this);
        visitor.endVisit(this);
	}

	public SQLName getTable() {
		return table;
	}

	public void setTable(SQLName table) {
		this.table = table;
	}

	public SQLName getSchema() {
		return schema;
	}

	public void setSchema(SQLName schema) {
		this.schema = schema;
	}

	public SQLExpr getParentTable() {
		return parentTable;
	}

	public void setParentTable(SQLExpr parentTable) {
		this.parentTable = parentTable;
	}

	public SQLExpr getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(SQLExpr primaryKey) {
		this.primaryKey = primaryKey;
	}

	public SQLExpr getParentKey() {
		return parentKey;
	}

	public void setParentKey(SQLExpr parentKey) {
		this.parentKey = parentKey;
	}

	public SQLExpr getJoinKey() {
		return joinKey;
	}

	public void setJoinKey(SQLExpr joinKey) {
		this.joinKey = joinKey;
	}

	public boolean isAutoIncrement() {
		return autoIncrement;
	}

	public void setAutoIncrement(boolean autoIncrement) {
		this.autoIncrement = autoIncrement;
	}
	
}
