package org.opencloudb.manager.parser.druid.statement;

import org.opencloudb.config.model.TableConfig;
import org.opencloudb.manager.parser.druid.MycatASTVisitor;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;
import com.google.common.base.Strings;

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
//	private boolean needAddLimit;
	
	@Override
	public void accept0(MycatASTVisitor visitor) {
		visitor.visit(this);
        visitor.endVisit(this);
	}
	
	public static MycatCreateChildTableStatement from(String schemaName, TableConfig tableConfig) {
	    if (tableConfig.getParentTC() == null) {
	        throw new IllegalArgumentException("table '" + tableConfig.getName().toLowerCase() + "' is not a childTable");
	    }
	    MycatCreateChildTableStatement stmt = new MycatCreateChildTableStatement();
	    stmt.setTable(new SQLIdentifierExpr(tableConfig.getName().toLowerCase()));
	    stmt.setParentTable(new SQLCharExpr(tableConfig.getParentTC().getName().toLowerCase()));
	    stmt.setPrimaryKey(new SQLCharExpr(tableConfig.getPrimaryKey()));
	    stmt.setParentKey(new SQLCharExpr(tableConfig.getParentKey()));
	    stmt.setJoinKey(new SQLCharExpr(tableConfig.getJoinKey()));
	    stmt.setAutoIncrement(tableConfig.isAutoIncrement());
	    if (!Strings.isNullOrEmpty(schemaName)) {
	        stmt.setSchema(new SQLIdentifierExpr(schemaName));
	    }
	    return stmt;
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

//	public boolean isNeedAddLimit() {
//		return needAddLimit;
//	}
//
//	public void setNeedAddLimit(boolean needAddLimit) {
//		this.needAddLimit = needAddLimit;
//	}
	
}
