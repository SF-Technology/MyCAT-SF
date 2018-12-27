package org.opencloudb.manager.parser.druid.statement;

import org.opencloudb.manager.parser.druid.MycatASTVisitor;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;

/**
 * 封装 drop table 语句解析结果
 * @author CrazyPig
 * @since 2017-02-05
 *
 */
public class MycatDropTableStatement extends MycatStatementImpl implements SQLDDLStatement {

	public SQLName table;
	
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
	
}
