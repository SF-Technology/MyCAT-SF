package org.opencloudb.manager.parser.druid.statement;

import java.util.ArrayList;
import java.util.List;

import org.opencloudb.manager.parser.druid.MycatASTVisitor;

import com.alibaba.druid.sql.ast.SQLExpr;

/**
 * 封装检查表结构一致性命令的语句
 * 
 * <pre>
 * sql format : <br>
 * check table structure consistency for schema_name[,schema_name1]*
 * </pre>
 * @author CrazyPig
 * @since 2016-09-07
 *
 */
public class MycatCheckTbStructConsistencyStatement extends MycatStatementImpl {
	
	private List<SQLExpr> nameList = new ArrayList<SQLExpr>();
	private SQLExpr where;

	public List<SQLExpr> getNameList() {
		return nameList;
	}

	public void setNameList(List<SQLExpr> nameList) {
		this.nameList = nameList;
	}
	
	public SQLExpr getWhere() {
		return where;
	}
	
	public void setWhere(SQLExpr where) {
		this.where = where;
	}

	@Override
	public void accept0(MycatASTVisitor visitor) {
		if(visitor.visit(this)) {
			acceptChild(visitor, getNameList());
		}
		visitor.endVisit(this);
	}
	
	
	
}
