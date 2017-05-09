package org.opencloudb.manager.parser.druid.statement;

import org.opencloudb.manager.parser.druid.MycatASTVisitor;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;

/**
 * 封装 create datanode 语句解析结果
 * @author CrazyPig
 * @since 2017-02-06
 *
 */
public class MycatCreateDataNodeStatement extends MycatStatementImpl implements SQLDDLStatement {

	private SQLName datanode;
	private SQLExpr datahost;
	private SQLExpr database;
	
	@Override
	public void accept0(MycatASTVisitor visitor) {
		visitor.visit(this);
        visitor.endVisit(this);
	}

	public SQLName getDatanode() {
		return datanode;
	}

	public void setDatanode(SQLName datanode) {
		this.datanode = datanode;
	}

	public SQLExpr getDatahost() {
		return datahost;
	}

	public void setDatahost(SQLExpr datahost) {
		this.datahost = datahost;
	}

	public SQLExpr getDatabase() {
		return database;
	}

	public void setDatabase(SQLExpr database) {
		this.database = database;
	}


}
