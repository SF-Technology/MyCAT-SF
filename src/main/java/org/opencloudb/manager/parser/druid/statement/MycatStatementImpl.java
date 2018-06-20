package org.opencloudb.manager.parser.druid.statement;

import org.opencloudb.MycatServer;
import org.opencloudb.manager.parser.druid.MycatASTVisitor;
import org.opencloudb.manager.parser.druid.MycatOutputVisitor;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatementImpl;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

public abstract class MycatStatementImpl extends SQLStatementImpl implements MycatStatement {

	public MycatStatementImpl() {
        super(MycatServer.NAME);
    }
	
	@Override
	protected void accept0(SQLASTVisitor visitor) {
		if (visitor instanceof MycatASTVisitor) {
            accept0((MycatASTVisitor) visitor);
        } else {
            throw new IllegalArgumentException("not support visitor type : " + visitor.getClass().getName());
        }
	}
	
	public void accept0(MycatASTVisitor visitor) {
        throw new UnsupportedOperationException(this.getClass().getName());
    }
	
	public String toString() {
		return toSQLString(this);
	}
	
	public final String toSQLString(SQLObject sqlObject) {
		StringBuilder out = new StringBuilder();
		sqlObject.accept(new MycatOutputVisitor(out));
		String sql = out.toString();
		return sql;
	}

}
