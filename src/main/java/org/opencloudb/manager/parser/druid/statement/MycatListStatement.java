package org.opencloudb.manager.parser.druid.statement;

import org.opencloudb.manager.parser.druid.MycatASTVisitor;

/**
 * list 语句解析结果
 * @author CrazyPig
 * @since 2017-02-08
 *
 */
public class MycatListStatement extends MycatStatementImpl {
	
	private MycatListStatementTarget target;
	
	@Override
	public void accept0(MycatASTVisitor visitor) {
		visitor.visit(this);
        visitor.endVisit(this);
	}
	
	public MycatListStatementTarget getTarget() {
		return target;
	}

	public void setTarget(MycatListStatementTarget target) {
		this.target = target;
	}
	
}
