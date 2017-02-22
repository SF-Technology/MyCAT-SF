package org.opencloudb.manager.parser.druid;

import java.util.ArrayList;
import java.util.List;

import org.opencloudb.MycatServer;
import org.opencloudb.manager.parser.druid.statement.MycatCheckTbStructConsistencyStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateDataNodeStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateSchemaStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateUserStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropDataHostStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropDataNodeStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropSchemaStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropTableStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropUserStatement;
import org.opencloudb.manager.parser.druid.statement.MycatListStatement;
import org.opencloudb.manager.parser.druid.statement.MycatListStatementTarget;
import org.opencloudb.parser.druid.MycatExprParser;
import org.opencloudb.parser.druid.MycatLexer;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.parser.Token;

/**
 * mycat管理端口命令解析器, 方便后期扩展更多自定义的管理命令
 * @author CrazyPig
 * @since 2016-09-07
 *
 */
public class MycatManageStatementParser extends SQLStatementParser {
	
	public MycatManageStatementParser(String sql) {
		this(sql, MycatServer.NAME);
	}
	
	public MycatManageStatementParser(String sql, String dbType) {
		super(new MycatExprParser(new MycatLexer(sql)));
		lexer.nextToken();
	}
	
	
	
	@Override
	public SQLStatement parseStatement() {
		List<SQLStatement> list = new ArrayList<SQLStatement>();
		super.parseStatementList(list);
		if(list.size() > 0) {
			return list.get(0);
		}
		return null;
	}

	@Override
	public void parseStatementList(List<SQLStatement> statementList, int max) {
		for (;;) {
            if (max != -1) {
                if (statementList.size() >= max) {
                    return;
                }
            }

            if (lexer.token() == Token.EOF) {
                return;
            }
            if (lexer.token() == Token.END) {
                return;
            }

            if (lexer.token() == (Token.SEMI)) {
                lexer.nextToken();
                continue;
            }

            if (lexer.token() == (Token.CREATE)) {
                statementList.add(parseCreate());
                continue;
            }

            if (lexer.token() == Token.SET) {
                statementList.add(parseSet());
                continue;
            }

            if (lexer.token() == Token.ALTER) {
                statementList.add(parseAlter());
                continue;
            }

            if (lexer.token() == Token.DROP) {
                lexer.nextToken();
                if (lexer.token() == Token.SCHEMA) {
                	statementList.add(parseDropSchema(false));
                    continue;
                } else if(lexer.token() == Token.TABLE) {
                    statementList.add(_parseDropTable(false));
                	continue;
                } else if(identifierEquals("DATANODE")) {
                	statementList.add(parseDropDataNode(false));
                	continue;
                } else if(identifierEquals("DATAHOST")) {
                	statementList.add(parseDropDataHost(false));
                } else if(lexer.token() == Token.USER) {
                	statementList.add(parseDropUser(false));
                	continue;
                } else {
                    throw new ParserException("TODO " + lexer.token());
                }
            }

            if (identifierEquals("RENAME")) {
                SQLStatement stmt = parseRename();
                statementList.add(stmt);
                continue;
            }

            if (parseStatementListDialect(statementList)) {
                continue;
            }

            if (lexer.token() == Token.COMMENT) {
                statementList.add(this.parseComment());
                continue;
            }

            // throw new ParserException("syntax error, " + lexer.token() + " "
            // + lexer.stringVal() + ", pos "
            // + lexer.pos());
            printError(lexer.token());
        }
	}
	
	/**
	 * create语句解析
	 */
	@Override
	public SQLStatement parseCreate() {
		
		accept(Token.CREATE);
		
		Token token = lexer.token();
		if(token == Token.SCHEMA) { // create schema
			
			return parseCreateSchema(false);
			
		} else if(token == Token.TABLE) { // create table
			
			MycatCreateTableParser createTableParser = new MycatCreateTableParser(this.exprParser);
			return createTableParser.parseCreateTable(false);
			
		} else if(identifierEquals("CHILDTABLE")) { // create childtable
			
			MycatCreateTableParser createTableParser = new MycatCreateTableParser(this.exprParser);
			return createTableParser.parseCreateChildTable(false);
			
		} else if(identifierEquals("DATANODE")) { // create datanode
			
			return parseCreateDataNode(false);
			
		} else if(identifierEquals("DATAHOST")) { // create datahost
			
			MycatCreateDataHostParser createDataHostParser = new MycatCreateDataHostParser(this.exprParser);
			return createDataHostParser.parseCreateDataHost(false);
			
		} else if(token == Token.USER) {
			
			return parseCreateUser(false);
			
		} else {
			
			throw new ParserException("Unsupport Statement : create " + token);
			
		}
		
	}
	
	public SQLStatement parseCreateUser(boolean acceptCreate) {
		if(acceptCreate) {
			accept(Token.CREATE);
		}
		accept(Token.USER);
		MycatCreateUserStatement stmt = new MycatCreateUserStatement();
		stmt.setUserName(this.exprParser.name());
		
		for(;;) {
			
			if(identifierEquals("PASSWORD")) {
				lexer.nextToken();
				accept(Token.EQ);
				stmt.setPassword(this.exprParser.expr());
				continue;
			}
			
			if(identifierEquals("SCHEMAS")) {
				acceptIdentifier("SCHEMAS");
				accept(Token.EQ);
				stmt.setSchemas(this.exprParser.expr());
				continue;
			}
			
			if(identifierEquals("READONLY")) {
				acceptIdentifier("READONLY");
				accept(Token.EQ);
				if(lexer.token() == Token.TRUE) {
					lexer.nextToken();
					stmt.setReadOnly(true);
				} else if(lexer.token() == Token.FALSE) {
					lexer.nextToken();
					stmt.setReadOnly(false);
				} else {
					throw new ParserException("readOnly must be true or false");
				}
				continue;
			}
			
			break;
		}
		
		// 语义检查
		if(stmt.getPassword() == null) {
			throw new ParserException("user definition must provide password property, eg: password = \"${your_password}\"");
		}
		
		if(stmt.getSchemas() == null) {
			throw new ParserException("user definition must provide schemas property, eg: schemas = \"${schemaList}\"");
		}
		
		return stmt;
	}
	
	/**
	 * create schema 语句解析
	 * @param acceptCreate
	 * @return
	 */
	public SQLStatement parseCreateSchema(boolean acceptCreate) {
		if(acceptCreate) {
			accept(Token.CREATE);
		}
		accept(Token.SCHEMA);
		MycatCreateSchemaStatement stmt = new MycatCreateSchemaStatement();
		stmt.setSchema(this.exprParser.name());
		
		for(;;) {
			
			if(identifierEquals("checkSQLschema")) {
				lexer.nextToken();
				accept(Token.EQ);
				if(lexer.token() == Token.TRUE) {
					lexer.nextToken();
					stmt.setCheckSQLSchema(true);
				} else if(lexer.token() == Token.FALSE) {
					lexer.nextToken();
					stmt.setCheckSQLSchema(false);
				} else {
					throw new ParserException("checkSQLschema must be true or false");
				}
				continue;
			}
			
			if(identifierEquals("sqlMaxLimit")) {
				lexer.nextToken();
				accept(Token.EQ);
				SQLExpr sqlExpr = this.exprParser.expr();
				if(sqlExpr instanceof SQLIntegerExpr) {
					stmt.setSqlMaxLimit(((SQLIntegerExpr)sqlExpr).getNumber().intValue());
				} else {
					throw new ParserException("sqlMaxLimit must be set as integer, eg: sqlMaxLimit = 1000");
				}
				continue;
			}
			
			break;
		}
		
		return stmt;
	}
	
	/**
	 * drop schema语句解析
	 * @param acceptDrop
	 * @return
	 */
	public SQLStatement parseDropSchema(boolean acceptDrop) {
		if(acceptDrop) {
			accept(Token.DROP);
		}
		accept(Token.SCHEMA);
		MycatDropSchemaStatement stmt = new MycatDropSchemaStatement();
		stmt.setSchema(this.exprParser.name());
		return stmt;
	}
	
	/**
	 * drop table语句解析
	 * @param acceptDrop
	 * @return
	 */
	public SQLStatement _parseDropTable(boolean acceptDrop) {
		if(acceptDrop) {
			accept(Token.DROP);
		}
		accept(Token.TABLE);
		MycatDropTableStatement stmt = new MycatDropTableStatement();
		stmt.setTable(this.exprParser.name());
		return stmt;
	}
	
	/**
	 * create datanode 语句解析
	 * @param acceptCreate
	 * @return
	 */
	public SQLStatement parseCreateDataNode(boolean acceptCreate) {
		if(acceptCreate) {
			accept(Token.CREATE);
		}
		acceptIdentifier("DATANODE");
		MycatCreateDataNodeStatement stmt = new MycatCreateDataNodeStatement();
		stmt.setDatanode(exprParser.name());
		
		for(;;) {
			
			if(identifierEquals("datahost")) {
				lexer.nextToken();
				accept(Token.EQ);
				stmt.setDatahost(this.exprParser.expr());
				continue;
			}
			
			if(lexer.token() == Token.DATABASE) {
				lexer.nextToken();
				accept(Token.EQ);
				stmt.setDatabase(this.exprParser.expr());
				continue;
			}
			
			break;
		}
		
		// 语义检查
		if(stmt.getDatahost() == null) {
			throw new ParserException("datanode definition must provide datahost property, eg: datahost = ${datahost}");
		}
		
		if(stmt.getDatabase() == null) {
			throw new ParserException("datanode definition must provide database property, eg: database = ${database}");
		}
		
		return stmt;
	}
	
	/**
	 * drop datanode语句解析
	 * @param acceptDrop
	 * @return
	 */
	public SQLStatement parseDropDataNode(boolean acceptDrop) {
		if(acceptDrop) {
			accept(Token.DROP);
		}
		acceptIdentifier("DATANODE");
		MycatDropDataNodeStatement stmt = new MycatDropDataNodeStatement();
		stmt.setDataNode(this.exprParser.name());
		return stmt;
	}
	
	/**
	 * drop datahost语句解析
	 * @param acceptDrop
	 * @return
	 */
	public SQLStatement parseDropDataHost(boolean acceptDrop) {
		if(acceptDrop) {
			accept(Token.DROP);
		}
		acceptIdentifier("DATAHOST");
		MycatDropDataHostStatement stmt = new MycatDropDataHostStatement();
		stmt.setDataHost(this.exprParser.name());
		return stmt;
	}
	
	public SQLStatement parseDropUser(boolean acceptDrop) {
		if(acceptDrop) {
			accept(Token.DROP);
		}
		accept(Token.USER);
		MycatDropUserStatement stmt = new MycatDropUserStatement();
		stmt.setUserName(this.exprParser.name());
		return stmt;
	}
	
	/**
	 * set语句解析
	 */
	@Override
	public SQLStatement parseSet() {
		// TODO parse set statement
		return null;
	}
	
	/**
	 * rename 语句解析
	 */
	public SQLStatement parseRename() {
		// TODO parse rename statement
		return null;
	}
	
	/**
	 * alter语句解析
	 */
	@Override
	public SQLStatement parseAlter() {
		throw new ParserException("Unsupport Alter statement");
	}

	/**
	 * TODO 在这里扩展自定义语法解析
	 */
	@Override
	public boolean parseStatementListDialect(List<SQLStatement> statementList) {

		if(lexer.token() == Token.CHECK) {
			accept(Token.CHECK);
			accept(Token.TABLE);
			if(identifierEquals("STRUCTURE")) {
				lexer.nextToken();
				statementList.add(parseCheckTableConsistencyStatement());
			}
			return true;
		}
		
		if(identifierEquals("LIST")) {
			lexer.nextToken();
			statementList.add(parseListStatement(false));
			return true;
		}
		
		return false;
	}
	
	/**
	 * 解析check table structure consistency语句
	 * @return
	 */
	public MycatCheckTbStructConsistencyStatement parseCheckTableConsistencyStatement() {
		acceptIdentifier("CONSISTENCY");
		MycatCheckTbStructConsistencyStatement stmt = new MycatCheckTbStructConsistencyStatement();
//		if(lexer.token() != Token.EOF) {
		accept(Token.FOR);
		stmt.setNameList(new ArrayList<SQLExpr>());
		this.exprParser.exprList(stmt.getNameList(), stmt);
		if (lexer.token() == Token.WHERE) {
			lexer.nextToken();
			stmt.setWhere(exprParser.expr());
		} else {
			accept(Token.EOF);
		}
//		}
		return stmt;
	}
	
	/**
	 * list 语句解析
	 * @param acceptList
	 * @return
	 */
	public MycatListStatement parseListStatement(boolean acceptList) {
		if(acceptList) {
			acceptIdentifier("LIST");
		}
		MycatListStatement stmt = new MycatListStatement();
		if(identifierEquals("SCHEMAS")) {
			stmt.setTarget(MycatListStatementTarget.SCHEMAS);
			lexer.nextToken();
		} else if(identifierEquals("TABLES")) {
			stmt.setTarget(MycatListStatementTarget.TABLES);
			lexer.nextToken();
		} else if(identifierEquals("DATANODES")) {
			stmt.setTarget(MycatListStatementTarget.DATANODES);
			lexer.nextToken();
		} else if(identifierEquals("DATAHOSTS")) {
			stmt.setTarget(MycatListStatementTarget.DATAHOSTS);
			lexer.nextToken();
		} else if(identifierEquals("RULES")) {
			stmt.setTarget(MycatListStatementTarget.RULES);
			lexer.nextToken();
		} else if(identifierEquals("FUNCTIONS")) {
			stmt.setTarget(MycatListStatementTarget.FUNCTIONS);
			lexer.nextToken();
		} else if(identifierEquals("USERS")) {
			stmt.setTarget(MycatListStatementTarget.USERS);
			lexer.nextToken();
		} else {
			throw new ParserException("Unsupport Statement : list " + lexer.stringVal());
		}
		return stmt;
	}

}
