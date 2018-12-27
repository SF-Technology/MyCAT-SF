package org.opencloudb.manager.parser.druid;

import org.opencloudb.manager.parser.druid.statement.MycatAlterUserStatement;
import org.opencloudb.manager.parser.druid.statement.MycatChecksumTableStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCheckTbStructConsistencyStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateChildTableStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateDataHostStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateDataNodeStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateFunctionStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateMapFileStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateProcedureStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateRuleStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateSchemaStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateTableStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateUserStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropDataHostStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropDataNodeStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropProcedureStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropSchemaStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropTableStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropUserStatement;
import org.opencloudb.manager.parser.druid.statement.MycatListStatement;

import com.alibaba.druid.sql.visitor.SQLASTVisitor;

public interface MycatASTVisitor extends SQLASTVisitor {
	
	boolean visit(MycatCheckTbStructConsistencyStatement stmt);
	void endVisit(MycatCheckTbStructConsistencyStatement stmt);
	
	boolean visit(MycatChecksumTableStatement stmt);
	void endVisit(MycatChecksumTableStatement stmt);
	
	boolean visit(MycatCreateSchemaStatement stmt);
	void endVisit(MycatCreateSchemaStatement stmt);

	boolean visit(MycatDropSchemaStatement stmt);
	void endVisit(MycatDropSchemaStatement stmt);
	
	boolean visit(MycatCreateTableStatement stmt);
	void endVisit(MycatCreateTableStatement stmt);
	
	boolean visit(MycatCreateChildTableStatement stmt);
	void endVisit(MycatCreateChildTableStatement stmt);
	
	boolean visit(MycatDropTableStatement stmt);
	void endVisit(MycatDropTableStatement stmt);
	
	boolean visit(MycatCreateDataNodeStatement stmt);
	void endVisit(MycatCreateDataNodeStatement stmt);
	
	boolean visit(MycatDropDataNodeStatement stmt);
	void endVisit(MycatDropDataNodeStatement stmt);
	
	boolean visit(MycatCreateDataHostStatement stmt);
	void endVisit(MycatCreateDataHostStatement stmt);
	
	boolean visit(MycatDropDataHostStatement stmt);
	void endVisit(MycatDropDataHostStatement stmt);
	
	void visit(MycatListStatement mycatListStatement);
	void endVisit(MycatListStatement stmt);
	
	void visit(MycatCreateUserStatement stmt);
	void endVisit(MycatCreateUserStatement stmt);
	
	void visit(MycatDropUserStatement stmt);
	void endVisit(MycatDropUserStatement stmt);
	
	void visit(MycatAlterUserStatement stmt);
	void endVisit(MycatAlterUserStatement stmt);
	
	void visit(MycatCreateFunctionStatement stmt);
    void endVisit(MycatCreateFunctionStatement stmt);
    
    void visit(MycatCreateRuleStatement stmt);
    void endVisit(MycatCreateRuleStatement stmt);
    
    void visit(MycatCreateMapFileStatement stmt);
    void endVisit(MycatCreateMapFileStatement stmt);
    
    void visit(MycatCreateProcedureStatement stmt);
    void endVisit(MycatCreateProcedureStatement stmt);
    
    void visit(MycatDropProcedureStatement stmt);
    void endVisit(MycatDropProcedureStatement stmt);
}
