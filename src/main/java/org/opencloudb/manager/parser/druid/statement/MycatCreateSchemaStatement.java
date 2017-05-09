package org.opencloudb.manager.parser.druid.statement;

import org.opencloudb.manager.parser.druid.MycatASTVisitor;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;

/**
 * 封装 create schema 语句解析结果
 * @author CrazyPig
 * @since 2017-02-05
 *
 */
public class MycatCreateSchemaStatement extends MycatStatementImpl implements SQLDDLStatement {

	public static final boolean DEFAULT_CHECK_SQL_SCHEMA = false;
	public static final int DEFAULT_SQL_MAX_LIMIT = 1000;
	
	private SQLName schema;
	private boolean checkSQLSchema = DEFAULT_CHECK_SQL_SCHEMA;
	private int sqlMaxLimit = DEFAULT_SQL_MAX_LIMIT;
	
	private String dataNode;
	
	@Override
	public void accept0(MycatASTVisitor visitor) {
		visitor.visit(this);
        visitor.endVisit(this);
	}

	public SQLName getSchema() {
		return schema;
	}

	public void setSchema(SQLName schema) {
		this.schema = schema;
	}

	public boolean isCheckSQLSchema() {
		return checkSQLSchema;
	}

	public void setCheckSQLSchema(boolean checkSQLSchema) {
		this.checkSQLSchema = checkSQLSchema;
	}

	public int getSqlMaxLimit() {
		return sqlMaxLimit;
	}

	public void setSqlMaxLimit(int sqlMaxLimit) {
		this.sqlMaxLimit = sqlMaxLimit;
	}

	public String getDataNode() {
		return dataNode;
	}

	public void setDataNode(String dataNode) {
		this.dataNode = dataNode;
	}

}
