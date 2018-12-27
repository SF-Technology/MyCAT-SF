package org.opencloudb.manager.parser.druid.statement;

import org.opencloudb.manager.parser.druid.MycatASTVisitor;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;

/**
 * 
 * @author 01169238
 * @since 1.5.3
 *
 */
public class MycatDropProcedureStatement extends MycatStatementImpl implements SQLDDLStatement {

	private SQLName procedure;
	
	@Override
	public void accept0(MycatASTVisitor visitor) {
		visitor.visit(this);
		visitor.endVisit(this);
	}

	public SQLName getProcedure() {
		return procedure;
	}

	public void setProcedure(SQLName procedure) {
		this.procedure = procedure;
	}
	
}
