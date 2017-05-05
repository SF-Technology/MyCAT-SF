package org.opencloudb.manager.parser.druid;

import java.util.ArrayList;
import java.util.List;

import org.opencloudb.manager.parser.druid.statement.MycatCreateMapFileStatement;
import org.opencloudb.util.StringUtil;

import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLDDLParser;
import com.alibaba.druid.sql.parser.SQLExprParser;
import com.alibaba.druid.sql.parser.Token;

public class MycatCreateMapFileParser extends SQLDDLParser {
	public MycatCreateMapFileParser(String sql) {
		super(sql);
	}
	
	public MycatCreateMapFileParser(SQLExprParser exprParser){
        super(exprParser);
    }
	
	public MycatCreateMapFileStatement parseCreateMapFile(boolean acceptCreate) {
		if (acceptCreate) {
			accept(Token.CREATE);
		}
		
		acceptIdentifier("MAPFILE");
		MycatCreateMapFileStatement stmt = new MycatCreateMapFileStatement();
		
		stmt.setFileName(acceptFileName());
		
		acceptIdentifier("INCLUDE");
		
		acceptIdentifier("LINES");
		
		stmt.setLines(acceptLines());
		
		return stmt;
	}
	
	/**
	 * 获得文件名
	 * @return
	 */
	private String acceptFileName() {
		String name;
		switch (lexer.token()){
		case LITERAL_ALIAS:
		case LITERAL_CHARS:
			name = lexer.stringVal();
			lexer.nextToken();
			break;
		default:
			name = StringUtil.removeBackquote(exprParser.name().getSimpleName());;
		}
		
		return name;
	}
	
	private List<String> acceptLines() {
		List<String> lines = new ArrayList<String>();
		
		accept(Token.LPAREN); // accept '('
		
		for(;;) {
			lines.add(acceptStr());
			
			if (lexer.token() == Token.COMMA){ // equals ','
				accept(Token.COMMA);
				continue;
			} else {
				accept(Token.RPAREN); // accept ')'
				break;
			}
		}
		
		return lines;
	}
	
	/**
	 * 只接收字符串
	 * @return
	 */
	private String acceptStr() {
		String str;
		switch (lexer.token()){
		case LITERAL_ALIAS:
		case LITERAL_CHARS:
			str = lexer.stringVal();
			lexer.nextToken();
			break;
		default:
			throw new ParserException("You have an error in syntax. String are expected. Get " + lexer.token() + ".");
		}
		
		return str;
	}
 }
