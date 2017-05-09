package org.opencloudb.manager.parser.druid.statement;

import org.opencloudb.manager.parser.druid.MycatASTVisitor;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;

/**
 * 封装 drop schema 语句解析结果
 * @author CrazyPig
 * @since 2017-02-05
 *
 */
public class MycatDropSchemaStatement extends MycatStatementImpl implements SQLDDLStatement {

	private SQLExpr schema;
	
	@Override
	public void accept0(MycatASTVisitor visitor) {
		visitor.visit(this);
        visitor.endVisit(this);
	}

	public SQLExpr getSchema() {
		return schema;
	}

	public void setSchema(SQLExpr schema) {
		this.schema = schema;
	}
	
}
