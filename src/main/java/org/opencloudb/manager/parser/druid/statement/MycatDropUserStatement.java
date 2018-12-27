package org.opencloudb.manager.parser.druid.statement;

import org.opencloudb.manager.parser.druid.MycatASTVisitor;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;

public class MycatDropUserStatement extends MycatStatementImpl implements SQLDDLStatement {
	
	private SQLName userName;

	@Override
	public void accept0(MycatASTVisitor visitor) {
		visitor.visit(this);
        visitor.endVisit(this);
	}
	
	public SQLName getUserName() {
		return userName;
	}

	public void setUserName(SQLName userName) {
		this.userName = userName;
	}
	
}
