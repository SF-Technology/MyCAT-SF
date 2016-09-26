package org.opencloudb.manager.parser.druid;

import java.util.ArrayList;
import java.util.List;

import org.opencloudb.MycatServer;
import org.opencloudb.manager.parser.druid.statement.MycatCheckTbStructConsistencyStatement;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
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
		super(sql, dbType);
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
	
	

}
