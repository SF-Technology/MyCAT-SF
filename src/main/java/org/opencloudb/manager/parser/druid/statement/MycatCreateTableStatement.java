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
public class MycatCreateTableStatement extends MycatStatementImpl implements SQLDDLStatement {

	private SQLName table;
	private boolean isGlobal;
	private boolean autoIncrement;
	private SQLExpr primaryKey;
	private SQLExpr dataNodes;
	private SQLExpr rule;
	private SQLName schema;
	private boolean needAddLimit = true; // needAddLimit默认为true, 参考XMLSchemaLoader
	
	@Override
	public void accept0(MycatASTVisitor visitor) {
		visitor.visit(this);
        visitor.endVisit(this);
	}
	
	public static MycatCreateTableStatement from(String schemaName, TableConfig tableConfig) {
	    if (tableConfig.getParentTC() != null) {
	        throw new IllegalArgumentException("table '" + tableConfig.getName().toLowerCase() + "' is a childTable");
	    }
	    MycatCreateTableStatement stmt = new MycatCreateTableStatement();
	    stmt.setTable(new SQLIdentifierExpr(tableConfig.getName().toLowerCase()));
	    stmt.setGlobal(tableConfig.isGlobalTable());
	    stmt.setAutoIncrement(tableConfig.isAutoIncrement());
	    stmt.setPrimaryKey(new SQLCharExpr(tableConfig.getPrimaryKey()));
	    stmt.setDataNodes(new SQLCharExpr(tableConfig.getDataNode()));
	    if (tableConfig.getRule() != null) {
	        stmt.setRule(new SQLCharExpr(tableConfig.getRule().getName()));
	    }
	    if (!Strings.isNullOrEmpty(schemaName)) {
	        stmt.setSchema(new SQLIdentifierExpr(schemaName));
	    }
	    stmt.setNeedAddLimit(tableConfig.isNeedAddLimit());
	    return stmt;
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

	public boolean isNeedAddLimit() {
		return needAddLimit;
	}

	public void setNeedAddLimit(boolean needAddLimit) {
		this.needAddLimit = needAddLimit;
	}
	
}
