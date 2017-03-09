package org.opencloudb.manager.parser.druid;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.opencloudb.MycatServer;
import org.opencloudb.manager.parser.druid.statement.MycatAlterUserStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCheckTbStructConsistencyStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateDataNodeStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateFunctionStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateRuleStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateSchemaStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateUserStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropDataHostStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropDataNodeStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropFunctionStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropRuleStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropSchemaStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropTableStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropUserStatement;
import org.opencloudb.manager.parser.druid.statement.MycatListStatement;
import org.opencloudb.manager.parser.druid.statement.MycatListStatementTarget;
import org.opencloudb.manager.parser.druid.statement.MycatSetSqlwallVariableStatement;
import org.opencloudb.manager.parser.druid.statement.MycatSetSystemVariableStatement;
import org.opencloudb.parser.druid.MycatExprParser;
import org.opencloudb.parser.druid.MycatLexer;
import org.opencloudb.util.StringUtil;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLDropFunctionStatement;
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
                } else if(identifierEquals("RULE")){
                	statementList.add(parseDropRule(false));
                	continue;
                } else if(lexer.token() == Token.FUNCTION) {
                	statementList.add(parseDropFunction(false));
                	continue;
                }else {
                    throw new ParserException("TODO " + lexer.token());
                }
            }

            if (identifierEquals("RENAME")) {
                SQLStatement stmt = parseRename();
                statementList.add(stmt);
                continue;
            }
            
            // 关于list的自定义语法解析入口 --stridehuan
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
			
		} else if(identifierEquals("RULE")){ // create rule
			
			return parseCreateRule(false);
		} else if(token == Token.FUNCTION){ // create function
			
			return parseCreateFunction(false);
			
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
		stmt.setUserName(new SQLIdentifierExpr(acceptName()));
		
		for(;;) {
			
			if(identifierEquals("PASSWORD")) {
				lexer.nextToken();
				accept(Token.EQ);
				stmt.setPassword(new SQLCharExpr(acceptNumAndStr()));
				continue;
			}
			
			if(identifierEquals("SCHEMAS")) {
				acceptIdentifier("SCHEMAS");
				accept(Token.EQ);
				stmt.setSchemas(new SQLCharExpr(acceptNumAndStr()));
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
		stmt.setSchema(new SQLIdentifierExpr(acceptName()));
		
		for(;;) {
			
			if(identifierEquals("dataNode")) {
				lexer.nextToken();
				accept(Token.EQ);
				stmt.setDataNode(acceptNumAndStr());
				continue;
			}
			
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
		
//		if(stmt.getDataNode() == null || stmt.getDataNode().isEmpty()) {
//			throw new ParserException("schema definition must provide default dataNode property, eg: dataNode = ${default_datanode}");
//		}
		
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
		stmt.setSchema(new SQLIdentifierExpr(acceptName()));
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
		stmt.setTable(new SQLIdentifierExpr(acceptName()));
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
		stmt.setDatanode(new SQLIdentifierExpr(acceptName()));
		
		for(;;) {
			
			if(identifierEquals("datahost")) {
				lexer.nextToken();
				accept(Token.EQ);
				stmt.setDatahost(new SQLCharExpr(acceptNumAndStr()));
				continue;
			}
			
			if(lexer.token() == Token.DATABASE) {
				lexer.nextToken();
				accept(Token.EQ);
				stmt.setDatabase(new SQLCharExpr(acceptNumAndStr()));
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
	 * create rule语句解析
	 * @param acceptCreate
	 * @return
	 */
	public SQLStatement parseCreateRule(boolean acceptCreate){
		if (acceptCreate){
			accept(Token.CREATE);
		}
		acceptIdentifier("RULE");
		MycatCreateRuleStatement stmt = new MycatCreateRuleStatement();
		stmt.setRule(acceptName());
		
		accept(Token.ON);
		accept(Token.COLUMN);
		stmt.setColumn(acceptName());
		
		acceptIdentifier("USING");
		accept(Token.FUNCTION);
		stmt.setFunction(acceptName()); 
		
		return stmt;
	}
	
	/**
	 * create function 解析
	 * @param acceptCreate
	 * @return
	 */
	public SQLStatement parseCreateFunction(boolean acceptCreate) {
		if (acceptCreate){
			accept(Token.CREATE);
		}
		accept(Token.FUNCTION);
		MycatCreateFunctionStatement stmt = new MycatCreateFunctionStatement();
		stmt.setFunction(acceptName());
		
		acceptIdentifier("USING");
		acceptIdentifier("CLASS");
		stmt.setClassName(acquireClassName());
		
		acceptIdentifier("INCLUDE");
		acceptIdentifier("PROPERTIES");
		stmt.setProperties(acquireProperties());
		
		return stmt;
	}
	
	/**
	 * 获得类的全路径
	 * 例：org.opencloudb.route.function.PartitionByHashMod 或
	 *  "org.opencloudb.route.function.PartitionByHashMod"
	 * @return
	 */
	private String acquireClassName(){
		boolean expectName = true;
		StringBuffer className = new StringBuffer();
		if (lexer.token() == Token.LITERAL_ALIAS || lexer.token() == Token.LITERAL_CHARS) {
			className.append(lexer.stringVal());
			lexer.nextToken();
			return className.toString();
		}
		
		for(;;){
			if (expectName){
				className.append(exprParser.name());
			} else {
				accept(Token.DOT);
				className.append('.');
			}
			
			expectName = !expectName;
			
			if (!expectName && lexer.token() != Token.DOT){
				break;
			}
		}
		
		return className.toString();
	}
	
	/**
	 * 获得properties
	 * 例：
	 * ({
	 * name=${prop_name} 
	 * value=${prop_value}
	 * }, {
	 * }, ...
	 * )
	 * @return
	 */
	private Map<String, String> acquireProperties(){
		Map<String, String> properties = new LinkedHashMap<String, String>();
		
		accept(Token.LPAREN); // accept '('
		
		for(;;){
			accept(Token.LBRACE); // accept '{'
			
			acceptIdentifier("NAME");
			accept(Token.EQ);
			String name = exprParser.name().getSimpleName();
			acceptIdentifier("VALUE");
			accept(Token.EQ);
			properties.put(name, acceptNumAndStr());
			
			accept(Token.RBRACE); // accept '}'
			
			if (lexer.token() == Token.COMMA){ // equals ','
				accept(Token.COMMA);
				continue;
			} else {
				accept(Token.RPAREN); // accept '}'
				break;
			}
		}
		
		return properties;
	}
	
	/**
	 * 只接受数值和字符串，如果是，返回相应字符串，如果不是，抛解析异常
	 * @return
	 */
	private String acceptNumAndStr() {
		String value;
		switch (lexer.token()){
		case LITERAL_INT:
		case LITERAL_FLOAT:
		case LITERAL_HEX:
			value = lexer.numberString();
			lexer.nextToken();
			break;
		case LITERAL_ALIAS:
		case LITERAL_CHARS:
		case TRUE:
		case FALSE:
			value = lexer.stringVal();
			lexer.nextToken();
			break;
		default:
			throw new ParserException("You have an error in syntax. Number or string are expected. Get " + lexer.token() + ".");
		}
		
		return value;
	}
	
	/**
	 * 接受一个name，比如rule name或function name等
	 * 不能包含特殊字符，如果包含特殊字符外面必须套 '`'
	 * @return
	 */
	private String acceptName() {
		String name;
		
		switch (lexer.token()){
		case LITERAL_ALIAS:
		case LITERAL_CHARS:
			throw new ParserException("You have an error in syntax. Name \"" + lexer.stringVal() + "\" is illegal.");
		default:
			name = StringUtil.removeBackquote(exprParser.name().getSimpleName());
			return name;
		}
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
		stmt.setDataNode(new SQLIdentifierExpr(acceptName()));
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
		stmt.setDataHost(new SQLIdentifierExpr(acceptName()));
		return stmt;
	}
	
	public SQLStatement parseDropUser(boolean acceptDrop) {
		if(acceptDrop) {
			accept(Token.DROP);
		}
		accept(Token.USER);
		MycatDropUserStatement stmt = new MycatDropUserStatement();
		stmt.setUserName(new SQLIdentifierExpr(acceptName()));
		return stmt;
	}
	
	/**
	 * drop rule语句解析
	 * @param acceptDrop
	 * @return
	 */
	public SQLStatement parseDropRule(boolean acceptDrop) {
		if(acceptDrop) {
			accept(Token.DROP);
		}
		acceptIdentifier("RULE");
		
		MycatDropRuleStatement stmt = new MycatDropRuleStatement();
		stmt.setRule(acceptName());
		
		return stmt;
	}
	
	/* 
	 * drop function语句解析
	 */
	@Override
    public SQLDropFunctionStatement parseDropFunction(boolean acceptDrop) {
        if (acceptDrop) {
            accept(Token.DROP);
        }

        MycatDropFunctionStatement stmt = new MycatDropFunctionStatement();

        accept(Token.FUNCTION);

        stmt.setName(new SQLIdentifierExpr(acceptName()));

        return stmt;
    }
	
	/**
	 * set语句解析
	 */
	@Override
	public SQLStatement parseSet() {
		accept(Token.SET);
		
		Token token = lexer.token();
		if(identifierEquals("SYSTEM")) { // create schema
			
			return parseSetSystemVariable(false);
			
		} else if(identifierEquals("SQLWALL")) {
			return parseSetSqlwallVariable(false);
		}
		else {
			
			throw new ParserException("Unsupport Statement : create " + token);
			
		}
	}
	
	/**
	 * set system variable语句
	 * @param acceptSet
	 * @return
	 */
	public SQLStatement parseSetSystemVariable(boolean acceptSet){
		if (acceptSet) {
			accept(Token.SET);
		}
		
		acceptIdentifier("SYSTEM");
		acceptIdentifier("VARIABLE");
		
		MycatSetSystemVariableStatement stmt = new MycatSetSystemVariableStatement();
		stmt.setVariableName(exprParser.name());
		
		accept(Token.EQ);
		
		stmt.setVariableValue(acceptNumAndStr());
		
		return stmt;
	}
	
	/**
	 * set sqlwall variable语句解析
	 * @param acceptSet
	 * @return
	 */
	public SQLStatement parseSetSqlwallVariable(boolean acceptSet){
		if(acceptSet) {
			accept(Token.SET);
		}
		
		acceptIdentifier("SQLWALL");
		acceptIdentifier("VARIABLE");
		
		MycatSetSqlwallVariableStatement stmt = new MycatSetSqlwallVariableStatement();
		stmt.setVariableName(exprParser.name());
		
		accept(Token.EQ);
		
		stmt.setVariableValue(acceptNumAndStr());
		
		return stmt;
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
//		throw new ParserException("Unsupport Alter statement");
		accept(Token.ALTER);
		Token token = lexer.token();
		if(token == Token.USER) {
			return parseAlterUser(false);
		} else {
			throw new ParserException("Unsupport Statement : alter " + token);
		}
	}
	
	public SQLStatement parseAlterUser(boolean acceptAlter) {
		if(acceptAlter) {
			accept(Token.ALTER);
		}
		accept(Token.USER);
		MycatAlterUserStatement stmt = new MycatAlterUserStatement();
		stmt.setUserName(new SQLIdentifierExpr(acceptName()));
		for(;;) {
			
			if(identifierEquals("PASSWORD")) {
				lexer.nextToken();
				accept(Token.EQ);
				stmt.setPassword(new SQLCharExpr(acceptNumAndStr()));
				stmt.setAlterPassword(true);
				continue;
			}
			
			if(identifierEquals("SCHEMAS")) {
				acceptIdentifier("SCHEMAS");
				accept(Token.EQ);
				stmt.setSchemas(new SQLCharExpr(acceptNumAndStr()));
				stmt.setAlterSchemas(true);
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
				stmt.setAlterReadOnly(true);
				continue;
			}
			
			break;
		}
		return stmt;
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
		} else if(identifierEquals("SYSTEM")) {
			acceptIdentifier("SYSTEM");
			acceptIdentifier("VARIABLES");
			stmt.setTarget(MycatListStatementTarget.SYSTEM_VARIABLES);
		} else if(identifierEquals("SQLWALL")) {
			acceptIdentifier("SQLWALL");
			acceptIdentifier("VARIABLES");
			stmt.setTarget(MycatListStatementTarget.SQLWALL_VARIABLES);
		} else {
			throw new ParserException("Unsupport Statement : list " + lexer.stringVal());
		}
		return stmt;
	}

}
