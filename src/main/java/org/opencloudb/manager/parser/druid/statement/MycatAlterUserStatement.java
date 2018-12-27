package org.opencloudb.manager.parser.druid.statement;

import org.opencloudb.manager.parser.druid.MycatASTVisitor;

/**
 * 
 * @author CrazyPig
 * @since 2017-02-23
 *
 */
public class MycatAlterUserStatement extends MycatCreateUserStatement {
	
	private boolean alterPassword;
	private boolean alterSchemas;
	private boolean alterReadOnly;
	
	@Override
	public void accept0(MycatASTVisitor visitor) {
		visitor.visit(this);
        visitor.endVisit(this);
	}
	
	public boolean isAlterPassword() {
		return alterPassword;
	}
	public void setAlterPassword(boolean alterPassword) {
		this.alterPassword = alterPassword;
	}
	public boolean isAlterSchemas() {
		return alterSchemas;
	}
	public void setAlterSchemas(boolean alterSchemas) {
		this.alterSchemas = alterSchemas;
	}
	public boolean isAlterReadOnly() {
		return alterReadOnly;
	}
	public void setAlterReadOnly(boolean alterReadOnly) {
		this.alterReadOnly = alterReadOnly;
	}
	
}
