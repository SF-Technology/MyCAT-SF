package org.opencloudb.manager.parser.druid.statement;

import org.opencloudb.manager.parser.druid.MycatASTVisitor;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;

/**
 * 封装 drop datahost 语句解析结果
 * @author CrazyPig
 * @since 2017-02-05
 *
 */
public class MycatDropDataHostStatement extends MycatStatementImpl implements SQLDDLStatement {

	private SQLName dataHost;
	
	@Override
	public void accept0(MycatASTVisitor visitor) {
		visitor.visit(this);
		visitor.endVisit(this);
	}

	public SQLName getDataHost() {
		return dataHost;
	}

	public void setDataHost(SQLName dataHost) {
		this.dataHost = dataHost;
	}
	
}
