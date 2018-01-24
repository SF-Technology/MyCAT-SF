package org.opencloudb.manager.parser.druid.statement;

import org.opencloudb.config.model.UserConfig;
import org.opencloudb.manager.parser.druid.MycatASTVisitor;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;
import com.google.common.base.Joiner;

public class MycatCreateUserStatement extends MycatStatementImpl implements SQLDDLStatement {
	
	private SQLName userName;
	private SQLExpr schemas;
	private SQLExpr password;
	private boolean readOnly = false;
	
	@Override
	public void accept0(MycatASTVisitor visitor) {
		visitor.visit(this);
        visitor.endVisit(this);
	}
	
	public static MycatCreateUserStatement from(UserConfig userConf) {
	    MycatCreateUserStatement stmt = new MycatCreateUserStatement();
	    stmt.setUserName(new SQLIdentifierExpr(userConf.getName()));
	    if (userConf.getSchemas().size() > 0) {
	        String schemas = Joiner.on(",").join(userConf.getSchemas());
	        stmt.setSchemas(new SQLCharExpr(schemas));
	    }
	    stmt.setPassword(new SQLCharExpr(userConf.getPassword()));
	    stmt.setReadOnly(userConf.isReadOnly());
	    return stmt;
	}
	
	public SQLName getUserName() {
		return userName;
	}
	public void setUserName(SQLName userName) {
		this.userName = userName;
	}
	public SQLExpr getSchemas() {
		return schemas;
	}
	public void setSchemas(SQLExpr schemas) {
		this.schemas = schemas;
	}
	public SQLExpr getPassword() {
		return password;
	}
	public void setPassword(SQLExpr password) {
		this.password = password;
	}
	public boolean isReadOnly() {
		return readOnly;
	}
	public void setReadOnly(boolean readOnly) {
		this.readOnly = readOnly;
	}
	

}
