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
public class MycatCreateTableStatement extends MycatStatementImpl implements SQLDDLStatement {

	private SQLName table;
	private boolean isGlobal;
	private boolean autoIncrement;
	private SQLExpr primaryKey;
	private SQLExpr dataNodes;
	private SQLExpr rule;
	private SQLName schema;
	
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

	public boolean isGlobal() {
		return isGlobal;
	}

	public void setGlobal(boolean isGlobal) {
		this.isGlobal = isGlobal;
	}

	public SQLExpr getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(SQLExpr primaryKey) {
		this.primaryKey = primaryKey;
	}

	public SQLExpr getDataNodes() {
		return dataNodes;
	}

	public void setDataNodes(SQLExpr dataNodes) {
		this.dataNodes = dataNodes;
	}

	public SQLExpr getRule() {
		return rule;
	}

	public void setRule(SQLExpr rule) {
		this.rule = rule;
	}

	public SQLName getSchema() {
		return schema;
	}

	public void setSchema(SQLName schema) {
		this.schema = schema;
	}

	public boolean isAutoIncrement() {
		return autoIncrement;
	}

	public void setAutoIncrement(boolean autoIncrement) {
		this.autoIncrement = autoIncrement;
	}
	
}
