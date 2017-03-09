package org.opencloudb.manager.parser.druid;

import org.opencloudb.manager.parser.druid.statement.MycatCreateDataHostStatement;
import org.opencloudb.util.StringUtil;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLDDLParser;
import com.alibaba.druid.sql.parser.SQLExprParser;
import com.alibaba.druid.sql.parser.Token;

/**
 * 解析Mycat create datahost 语句的解析器
 * @author CrazyPig
 * @since 2017-02-07
 *
 */
public class MycatCreateDataHostParser extends SQLDDLParser {

	public MycatCreateDataHostParser(String sql) {
		super(sql);
	}
	
	public MycatCreateDataHostParser(SQLExprParser exprParser){
        super(exprParser);
    }
	
	/**
	 * create datahost 语句解析
	 * @param acceptCreate
	 * @return
	 */
	public MycatCreateDataHostStatement parseCreateDataHost(boolean acceptCreate) {
		if(acceptCreate) {
			accept(Token.CREATE);
		}
		acceptIdentifier("DATAHOST");
		MycatCreateDataHostStatement stmt = new MycatCreateDataHostStatement();
		stmt.setDatahost(new SQLIdentifierExpr(acceptName()));
		
		for(;;) {
			
			if(identifierEquals("maxCon")) {
				lexer.nextToken();
				accept(Token.EQ);
				SQLExpr maxCon = this.exprParser.expr();
				if(maxCon instanceof SQLIntegerExpr) {
					stmt.setMaxCon((SQLIntegerExpr) maxCon);
				} else {
					throw new ParserException("maxCon must be set as integer");
				}
				continue;
			}
			
			if(identifierEquals("minCon")) {
				lexer.nextToken();
				accept(Token.EQ);
				SQLExpr minCon = this.exprParser.expr();
				if(minCon instanceof SQLIntegerExpr) {
					stmt.setMinCon((SQLIntegerExpr) minCon);
				} else {
					throw new ParserException("minCon must be set as integer");
				}
				continue;
			}
			
			if(identifierEquals("balance")) {
				lexer.nextToken();
				accept(Token.EQ);
				SQLExpr balance = this.exprParser.expr();
				if(balance instanceof SQLIntegerExpr) {
					stmt.setBalance((SQLIntegerExpr) balance);
				} else {
					throw new ParserException("balance must be set as integer");
				}
				continue;
			}
			
			if(identifierEquals("dbType")) {
				lexer.nextToken();
				accept(Token.EQ);
				stmt.setmDbType(new SQLCharExpr(acceptStringVal()));
				continue;
			}
			
			if(identifierEquals("dbDriver")) {
				lexer.nextToken();
				accept(Token.EQ);
				stmt.setDbDriver(new SQLCharExpr(acceptStringVal()));
				continue;
			}
			
			if(identifierEquals("switchType")) {
				lexer.nextToken();
				accept(Token.EQ);
				SQLExpr switchType = this.exprParser.expr();
				if(switchType instanceof SQLIntegerExpr) {
					stmt.setSwitchType((SQLIntegerExpr)switchType);
				} else {
					throw new ParserException("switchType must be set as integer");
				}
				continue;
			}
			
			if(lexer.token() == Token.WITH) {
				parseWriteHosts(true, stmt);
				continue;
			}
			
			break;
		}
		
		// 语义检查
		if(stmt.getWriteHosts().size() == 0) {
			throw new ParserException("datahost definition must provide at least one writeHost");
		}
		
		return stmt;
	}
	
	/**
	 * writeHost 子句解析
	 * @param acceptWith
	 * @param stmt
	 * @return
	 */
	private void parseWriteHosts(boolean acceptWith, MycatCreateDataHostStatement stmt) {
		
		if(acceptWith) {
			accept(Token.WITH);
		}
		
		acceptIdentifier("writeHosts");
		
		accept(Token.LPAREN); // accept (
		
		for(;;) {
			
			accept(Token.LBRACE); // accept {
			
			MycatCreateDataHostStatement.Host writeHost = stmt.new Host();
			
			for(;;) {
				
				if(identifierEquals("host")) {
					lexer.nextToken();
					accept(Token.EQ);
					writeHost.setHost(new SQLCharExpr(acceptStringVal()));
					continue;
				}
				
				if(identifierEquals("url")) {
					lexer.nextToken();
					accept(Token.EQ);
					writeHost.setUrl(new SQLCharExpr(acceptStringVal()));
					continue;
				}
				
				if(lexer.token() == Token.USER) {
					lexer.nextToken();
					accept(Token.EQ);
					writeHost.setUser(new SQLCharExpr(acceptStringVal()));
					continue;
				}
				
				if(identifierEquals("password")) {
					lexer.nextToken();
					accept(Token.EQ);
					writeHost.setPassword(new SQLCharExpr(acceptStringVal()));
					continue;
				}
				
				if(lexer.token() == Token.WITH) {
					parseReadHosts(true, stmt, writeHost);
					continue;
				}
				
				break;
			}
			
			accept(Token.RBRACE); // accept }
			
			// 语义检查
			if(writeHost.getHost() == null) {
				throw new ParserException("writeHost definition must provide host property, eg: host = ${host}");
			}
			
			if(writeHost.getUrl() == null) {
				throw new ParserException("writeHost definition must provide url property, eg: url = ${url}");
			}
			
			if(writeHost.getUser() == null) {
				throw new ParserException("writeHost definition must provide user property, eg: user = ${user}");
			}
			
			if(writeHost.getPassword() == null) {
				throw new ParserException("writeHost definition must provide password property, eg: password = ${password}");
			}
			
			stmt.getWriteHosts().add(writeHost);
					
			Token token = lexer.token();
			if(token == Token.COMMA) { // need to parse more
				accept(Token.COMMA);
				continue;
			} else if(token != Token.RPAREN) { // must be )
				printError(token);
			}
			
			break;
		}
		
		accept(Token.RPAREN); // accept )
	}
	
	/**
	 * readHost 子句解析
	 * @param acceptWith
	 * @param stmt
	 * @param writeHost
	 */
	private void parseReadHosts(boolean acceptWith, MycatCreateDataHostStatement stmt, MycatCreateDataHostStatement.Host writeHost) {
		if(acceptWith) {
			accept(Token.WITH);
		}
		
		acceptIdentifier("readHosts");
		
		accept(Token.LPAREN); // accept ( 
		
		for(;;) {
			
			accept(Token.LBRACE); // accept {
			
			MycatCreateDataHostStatement.Host readHost = stmt.new Host();
			
			for(;;) {
				
				if(identifierEquals("host")) {
					lexer.nextToken();
					accept(Token.EQ);
					readHost.setHost(new SQLCharExpr(acceptStringVal()));
					continue;
				}
				
				if(identifierEquals("url")) {
					lexer.nextToken();
					accept(Token.EQ);
					readHost.setUrl(new SQLCharExpr(acceptStringVal()));
					continue;
				}
				
				if(lexer.token() == Token.USER) {
					lexer.nextToken();
					accept(Token.EQ);
					readHost.setUser(new SQLCharExpr(acceptStringVal()));
					continue;
				}
				
				if(identifierEquals("password")) {
					lexer.nextToken();
					accept(Token.EQ);
					readHost.setPassword(new SQLCharExpr(acceptStringVal()));
					continue;
				}
				
				break;
			}
			
			accept(Token.RBRACE); // accept }
			
			// 语义检查
			if(readHost.getHost() == null) {
				throw new ParserException("readHost definition must provide host property, eg: host = ${host}");
			}
			
			if(readHost.getUrl() == null) {
				throw new ParserException("readHost definition must provide url property, eg: url = ${url}");
			}
			
			if(readHost.getUser() == null) {
				throw new ParserException("readHost definition must provide user property, eg: user = ${user}");
			}
			
			if(readHost.getPassword() == null) {
				throw new ParserException("readHost definition must provide password property, eg: password = ${password}");
			}
			
			writeHost.getReadHosts().add(readHost);
			
			Token token = lexer.token();
			if(token == Token.COMMA) { // need to parse more
				accept(Token.COMMA);
				continue;
			} else if(token != Token.RPAREN) { // must be )
				printError(token);
			}
			
			break;
			
		}
		
		accept(Token.RPAREN); // accept )
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
	 * 只接受字符串，如果是，返回相应字符串，如果不是，抛解析异常
	 * @return
	 */
	private String acceptStringVal() {
		String value;
		switch (lexer.token()){
		case LITERAL_ALIAS:
		case LITERAL_CHARS:
			value = lexer.stringVal();
			lexer.nextToken();
			break;
		default:
			throw new ParserException("error " + lexer.token());
		}
		
		return value;
	}
	
}
