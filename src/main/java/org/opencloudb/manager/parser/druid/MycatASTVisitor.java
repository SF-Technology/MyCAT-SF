package org.opencloudb.manager.parser.druid;

import org.opencloudb.manager.parser.druid.statement.MycatChecksumTableStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCheckTbStructConsistencyStatement;

import com.alibaba.druid.sql.visitor.SQLASTVisitor;

public interface MycatASTVisitor extends SQLASTVisitor {
	
	boolean visit(MycatCheckTbStructConsistencyStatement stmt);
	void endVisit(MycatCheckTbStructConsistencyStatement stmt);
	
	boolean visit(MycatChecksumTableStatement stmt);
	void endVisit(MycatChecksumTableStatement stmt);

}
