package org.opencloudb.manager.parser.druid;

import java.util.List;

import org.opencloudb.manager.parser.druid.statement.MycatAlterUserStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCheckTbStructConsistencyStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateChildTableStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateDataHostStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateDataNodeStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateSchemaStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateTableStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateUserStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropDataHostStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropDataNodeStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropSchemaStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropTableStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropUserStatement;
import org.opencloudb.manager.parser.druid.statement.MycatListStatement;

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

	@Override
	public boolean visit(MycatDropSchemaStatement stmt) {
		print("DROP SCHEMA " + stmt.getSchema());
		return false;
	}

	@Override
	public void endVisit(MycatDropSchemaStatement stmt) {
		
	}

	@Override
	public boolean visit(MycatDropTableStatement stmt) {
		print("DROP SCHEMA " + stmt.getTable());
		return false;
	}

	@Override
	public void endVisit(MycatDropTableStatement stmt) {
		
	}

	@Override
	public boolean visit(MycatDropDataNodeStatement stmt) {
		print("DROP DATANODE " + stmt.getDataNode());
		return false;
	}

	@Override
	public void endVisit(MycatDropDataNodeStatement stmt) {
		
	}

	@Override
	public boolean visit(MycatDropDataHostStatement stmt) {
		print("DROP DATAHOST " + stmt.getDataHost());
		return false;
	}

	@Override
	public void endVisit(MycatDropDataHostStatement stmt) {
		
	}

	@Override
	public boolean visit(MycatCreateSchemaStatement stmt) {
		print("CREATE SCHEMA " + stmt.getSchema());
		print(" checkSQLschema = " + stmt.isCheckSQLSchema());
		print(" sqlMaxLimit = " + stmt.getSqlMaxLimit());
		println();
		return false;
	}

	@Override
	public void endVisit(MycatCreateSchemaStatement stmt) {
		
	}

	@Override
	public boolean visit(MycatCreateTableStatement stmt) {
		print("CREATE TABLE " + stmt.getTable().getSimpleName());
		if(stmt.getSchema() != null) {
			print(" IN " + stmt.getSchema().getSimpleName());
		}
		println();
		if(stmt.isGlobal()) {
			println("global = " + stmt.isGlobal());
		}
		if(stmt.getPrimaryKey() != null) {
			println("primaryKey = " + stmt.getPrimaryKey());
		}
		println("dataNode = " + stmt.getDataNodes());
		if(stmt.getRule() != null) {
			println("rule = " + stmt.getRule());
		}
		return false;
	}

	@Override
	public void endVisit(MycatCreateTableStatement stmt) {
		
	}

	@Override
	public boolean visit(MycatCreateChildTableStatement stmt) {
		print("CREATE CHILDTABLE " + stmt.getTable().getSimpleName());
		if(stmt.getSchema() != null) {
			print(" IN " + stmt.getSchema().getSimpleName());
		}
		println();
		println("parent = " + stmt.getParent());
		println("parentKey = " + stmt.getParentKey());
		println("joinKey = " + stmt.getJoinKey());
		if(stmt.getPrimaryKey() != null) {
			println("primaryKey = " + stmt.getPrimaryKey());
		}
		return false;
	}

	@Override
	public void endVisit(MycatCreateChildTableStatement stmt) {
		
	}

	@Override
	public boolean visit(MycatCreateDataNodeStatement stmt) {
		print("CREATE DATANODE " + stmt.getDatanode().getSimpleName());
		print(" datahost = " + stmt.getDatahost());
		print(" database = " + stmt.getDatabase());
		return false;
	}

	@Override
	public void endVisit(MycatCreateDataNodeStatement stmt) {
		
	}

	@Override
	public boolean visit(MycatCreateDataHostStatement stmt) {
		println("CREATE DATAHOST " + stmt.getDatahost().getSimpleName());
		println("maxCon = " + stmt.getMaxCon());
		println("minCon = " + stmt.getMinCon());
		println("balance = " + stmt.getBalance());
		println("dbType = " + stmt.getmDbType());
		println("dbDriver = " + stmt.getDbDriver());
		println("switchType = " + stmt.getSwitchType());
		println("WITH writeHosts ( ");
		for(int i = 0, n = stmt.getWriteHosts().size(); i < n; i++) {
			MycatCreateDataHostStatement.Host writeHost = stmt.getWriteHosts().get(i);
			print("  {");
			print(" host = " + writeHost.getHost());
			print(" url = " + writeHost.getUrl());
			print(" user = " + writeHost.getUser());
			print(" password = " + writeHost.getPassword());
			
			if(writeHost.getReadHosts().size() > 0) {
				
				print(" WITH readHosts ( ");
				
				for(int j = 0, m = writeHost.getReadHosts().size(); j < m; j++) {
					MycatCreateDataHostStatement.Host readHost = writeHost.getReadHosts().get(j);
					print(" {");
					print(" host = " + readHost.getHost());
					print(" url = " + readHost.getUrl());
					print(" user = " + readHost.getUser());
					print(" password = " + readHost.getPassword());
					print(" }");
					
					if(j != m - 1) {
						print(", ");
					}
					
				}
				
				print(" )");
				
			}
			
			print(" }");
			
			if(i != n - 1) {
				println(",");
			}
			
		}
		println(" )");
		return false;
	}

	@Override
	public void endVisit(MycatCreateDataHostStatement stmt) {
		
	}

	@Override
	public void visit(MycatListStatement stmt) {
		print("LIST " + stmt.getTarget().name());
	}

	@Override
	public void endVisit(MycatListStatement stmt) {
		
	}

	@Override
	public void visit(MycatCreateUserStatement stmt) {
		println("CREATE USER " + stmt.getUserName().getSimpleName());
		println("password = " + stmt.getPassword());
		println("schemas = " + stmt.getSchemas());
		println("readOnly = " + stmt.isReadOnly());
	}

	@Override
	public void endVisit(MycatCreateUserStatement stmt) {
		
	}

	@Override
	public void visit(MycatDropUserStatement stmt) {
		print("DROP USER " + stmt.getUserName());
	}

	@Override
	public void endVisit(MycatDropUserStatement stmt) {
		
	}

	@Override
	public void visit(MycatAlterUserStatement stmt) {
		println("ALTER USER " + stmt.getUserName().getSimpleName());
		if(stmt.isAlterPassword()) {
			println("password = " + stmt.getPassword());
		}
		if(stmt.isAlterSchemas()) {
			println("schemas = " + stmt.getSchemas());
		}
		if(stmt.isAlterReadOnly()) {
			println("readOnly = " + stmt.isReadOnly());
		}
	}

	@Override
	public void endVisit(MycatAlterUserStatement stmt) {
		
	}

}
