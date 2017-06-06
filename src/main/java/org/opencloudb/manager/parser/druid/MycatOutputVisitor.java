package org.opencloudb.manager.parser.druid;

import java.util.List;

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

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;

public class MycatOutputVisitor extends SQLASTOutputVisitor implements MycatASTVisitor {

    public static final String MYCAT_CONFIG_SQL_PREFIX = "MYCAT_CONFIG";
    public static final String SQL_EMIC = ";";

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
	public boolean visit(MycatChecksumTableStatement stmt) {
		print("CHECKSUM TABLE ");
		stmt.getTableName().accept(this);
		return false;
	}
	
	@Override
	public void endVisit(MycatChecksumTableStatement stmt) {
		
	}

	@Override
	public boolean visit(MycatDropSchemaStatement stmt) {
		print(MYCAT_CONFIG_SQL_PREFIX + " DROP SCHEMA " + stmt.getSchema());
		return false;
	}
	
	@Override
	public void endVisit(MycatDropSchemaStatement stmt) {
		
	}

	@Override
	public boolean visit(MycatDropTableStatement stmt) {
		print(MYCAT_CONFIG_SQL_PREFIX + " DROP SCHEMA " + stmt.getTable());
		return false;
	}

	@Override
	public void endVisit(MycatDropTableStatement stmt) {
		
	}

	@Override
	public boolean visit(MycatDropDataNodeStatement stmt) {
		print(MYCAT_CONFIG_SQL_PREFIX + " DROP DATANODE " + stmt.getDataNode());
		return false;
	}

	@Override
	public void endVisit(MycatDropDataNodeStatement stmt) {
		
	}

	@Override
	public boolean visit(MycatDropDataHostStatement stmt) {
		print(MYCAT_CONFIG_SQL_PREFIX + " DROP DATAHOST " + stmt.getDataHost());
		return false;
	}

	@Override
	public void endVisit(MycatDropDataHostStatement stmt) {
		
	}

	@Override
	public boolean visit(MycatCreateSchemaStatement stmt) {
		print(MYCAT_CONFIG_SQL_PREFIX + " CREATE SCHEMA `" + stmt.getSchema() + "`");
		if (stmt.getDataNode() != null) {
		    print(" dataNode = \"" + stmt.getDataNode() + "\"");
		}
		print(" checkSQLschema = " + stmt.isCheckSQLSchema());
		print(" sqlMaxLimit = " + stmt.getSqlMaxLimit());
		return false;
	}

	@Override
	public void endVisit(MycatCreateSchemaStatement stmt) {
	    println(SQL_EMIC);
	}

	@Override
	public boolean visit(MycatCreateTableStatement stmt) {
		print(MYCAT_CONFIG_SQL_PREFIX + " CREATE TABLE `" + stmt.getTable().getSimpleName() + "`");
		if(stmt.getSchema() != null) {
			print(" IN `" + stmt.getSchema().getSimpleName() + "`");
		}
		if(stmt.isGlobal()) {
			print(" global = " + stmt.isGlobal());
		}
		if(stmt.getPrimaryKey() != null) {
			print(" primaryKey = " + stmt.getPrimaryKey());
		}
		print(" autoIncrement = " + stmt.isAutoIncrement());
		print(" dataNode = " + stmt.getDataNodes());
		if(stmt.getRule() != null) {
			print(" rule = " + stmt.getRule());
		}
		print(" needAddLimit = " + stmt.isNeedAddLimit());
		return false;
	}

	@Override
	public void endVisit(MycatCreateTableStatement stmt) {
		println(SQL_EMIC);
	}

	@Override
	public boolean visit(MycatCreateChildTableStatement stmt) {
		print(MYCAT_CONFIG_SQL_PREFIX + " CREATE CHILDTABLE `" + stmt.getTable().getSimpleName() + "`");
		if(stmt.getSchema() != null) {
			print(" IN `" + stmt.getSchema().getSimpleName() + "`");
		}
		print(" parent = " + stmt.getParentTable());
		print(" parentKey = " + stmt.getParentKey());
		print(" joinKey = " + stmt.getJoinKey());
		if(stmt.getPrimaryKey() != null) {
			print(" primaryKey = " + stmt.getPrimaryKey());
		}
		print(" autoIncrement = " + stmt.isAutoIncrement());
		return false;
	}

	@Override
	public void endVisit(MycatCreateChildTableStatement stmt) {
	    println(SQL_EMIC);
	}

	@Override
	public boolean visit(MycatCreateDataNodeStatement stmt) {
		print(MYCAT_CONFIG_SQL_PREFIX + " CREATE DATANODE `" + stmt.getDatanode().getSimpleName() + "`");
		print(" datahost = " + stmt.getDatahost());
		print(" database = " + stmt.getDatabase());
		return false;
	}

	@Override
	public void endVisit(MycatCreateDataNodeStatement stmt) {
	    println(SQL_EMIC);
	}

	@Override
	public boolean visit(MycatCreateDataHostStatement stmt) {
		println(MYCAT_CONFIG_SQL_PREFIX + " CREATE DATAHOST `" + stmt.getDatahost() + "`");
		println("  maxCon = " + stmt.getMaxCon());
		println("  minCon = " + stmt.getMinCon());
		println("  balance = " + stmt.getBalance());
		println("  dbType = " + stmt.getmDbType());
		println("  dbDriver = " + stmt.getDbDriver());
		println("  switchType = " + stmt.getSwitchType());
		println("  WITH writeHosts ( ");
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
		print(" )");
		return false;
	}

	@Override
	public void endVisit(MycatCreateDataHostStatement stmt) {
	    println(SQL_EMIC);
	}

	@Override
	public void visit(MycatListStatement stmt) {
		print(MYCAT_CONFIG_SQL_PREFIX + " LIST " + stmt.getTarget().name());
	}

	@Override
	public void endVisit(MycatListStatement stmt) {
	    println(SQL_EMIC);
	}

	@Override
	public void visit(MycatCreateUserStatement stmt) {
		print(MYCAT_CONFIG_SQL_PREFIX + " CREATE USER `" + stmt.getUserName().getSimpleName() + "`");
		print(" password = " + stmt.getPassword());
		print(" schemas = " + stmt.getSchemas());
		print(" readOnly = " + stmt.isReadOnly());
	}

	@Override
	public void endVisit(MycatCreateUserStatement stmt) {
	    println(SQL_EMIC);
	}

	@Override
	public void visit(MycatDropUserStatement stmt) {
		print(MYCAT_CONFIG_SQL_PREFIX + " DROP USER " + stmt.getUserName());
	}

	@Override
	public void endVisit(MycatDropUserStatement stmt) {
		
	}

	@Override
	public void visit(MycatAlterUserStatement stmt) {
		println(MYCAT_CONFIG_SQL_PREFIX + " ALTER USER " + stmt.getUserName().getSimpleName());
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

    @Override
    public void visit(MycatCreateFunctionStatement stmt) {
        print(MYCAT_CONFIG_SQL_PREFIX + " CREATE FUNCTION `" + stmt.getFunction() + "`");
        print(" USING CLASS \'" + stmt.getClassName() + "'");
        println(" INCLUDE PROPERTIES (");
        int size = stmt.getProperties().size();
        int count = 0;
        for (String key : stmt.getProperties().keySet()) {
            String value = stmt.getProperties().get(key);
            if (count == size - 1) {
                println("{name = '" + key + "' value = '" + value + "'}");
            } else {
                println("{name = '" + key + "' value = '" + value + "'},");
            }
            count++;
        }
        print(")");
    }
    
    @Override
    public void endVisit(MycatCreateFunctionStatement stmt) {
        println(SQL_EMIC);
    }

    @Override
    public void visit(MycatCreateRuleStatement stmt) {
        print(MYCAT_CONFIG_SQL_PREFIX + " CREATE RULE `" + stmt.getRule() + "`");
        print(" ON COLUMN `" + stmt.getColumn() + "`");
        print(" USING FUNCTION `" + stmt.getFunction() + "`");
    }

    @Override
    public void endVisit(MycatCreateRuleStatement stmt) {
        println(SQL_EMIC);
    }

    @Override
    public void visit(MycatCreateMapFileStatement stmt) {
        print(MYCAT_CONFIG_SQL_PREFIX + " CREATE MAPFILE \"" + stmt.getFileName() + "\"");
        println(" INCLUDE LINES (");
        if (stmt.getLines() != null && stmt.getLines().size() > 0) {
            for (int i = 0, n = stmt.getLines().size(); i < n; i++) {
                String line = stmt.getLines().get(i);
                if (i == n - 1) {
                    println("\"" + line + "\"");
                } else {
                    println("\"" + line + "\",");
                }
            }
        }
        print(")");
    }

    @Override
    public void endVisit(MycatCreateMapFileStatement stmt) {
        println(SQL_EMIC);
    }

    @Override
    public void visit(MycatCreateProcedureStatement stmt) {
        print(MYCAT_CONFIG_SQL_PREFIX + " CREATE PROCEDURE `" + stmt.getProcedure() + "`");
        if (stmt.getSchema() != null) {
            print(" IN `" + stmt.getSchema().getSimpleName() + "`");
        }
        print(" dataNode = \"" + stmt.getDataNodes() + "\"");
    }

    @Override
    public void endVisit(MycatCreateProcedureStatement stmt) {
        println(SQL_EMIC);        
    }

    @Override
    public void visit(MycatDropProcedureStatement stmt) {
        print(MYCAT_CONFIG_SQL_PREFIX + " DROP PROCEDURE `" + stmt.getProcedure() + "`");
    }

    @Override
    public void endVisit(MycatDropProcedureStatement stmt) {
        println(SQL_EMIC);
    }
    
}
