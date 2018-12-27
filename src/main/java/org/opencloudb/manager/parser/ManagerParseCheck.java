package org.opencloudb.manager.parser;

import org.opencloudb.manager.parser.druid.MycatManageStatementParser;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.parser.ParserException;

/**
 * Manager端口 check语句解析
 * @author CrazyPig
 * @since 2016-09-08
 *
 */
public class ManagerParseCheck {
	
	public static SQLStatement parse(String sql) throws ParserException {
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		SQLStatement stmt = parser.parseStatement();
		return stmt;
	}

}
