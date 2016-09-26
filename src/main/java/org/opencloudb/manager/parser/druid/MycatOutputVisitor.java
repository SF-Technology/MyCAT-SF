package org.opencloudb.manager.parser.druid;

import java.util.List;

import org.opencloudb.manager.parser.druid.statement.MycatCheckTbStructConsistencyStatement;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;

public class MycatOutputVisitor extends SQLASTOutputVisitor implements MycatASTVisitor {

	public MycatOutputVisitor(Appendable appender) {
		super(appender);
	}

	@Override
	public boolean visit(MycatCheckTbStructConsistencyStatement stmt) {
		print("CHECK TABLE STRUCTURE CONSISTENCY");
		List<SQLExpr> nameList = stmt.getNameList();
		int size = nameList.size();
		if(size > 0) {
			print(" FOR ");
			printAndAccept(nameList, ", ");
		}
		return false;
	}

	@Override
	public void endVisit(MycatCheckTbStructConsistencyStatement stmt) {
		
	}

}
