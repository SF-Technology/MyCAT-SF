package org.opencloudb.manager.parser.druid.statement;

import org.opencloudb.manager.parser.druid.MycatASTVisitor;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;

/**
 * 封装 drop datanode 语句解析结果
 * @author CrazyPig
 * @since 2017-02-05
 *
 */
public class MycatDropDataNodeStatement extends MycatStatementImpl implements SQLDDLStatement {

	private SQLName dataNode;
	
	@Override
	public void accept0(MycatASTVisitor visitor) {
		visitor.visit(this);
		visitor.endVisit(this);
	}

	public SQLName getDataNode() {
		return dataNode;
	}

	public void setDataNode(SQLName dataNode) {
		this.dataNode = dataNode;
	}
	
}
