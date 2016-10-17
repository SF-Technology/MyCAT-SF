package org.opencloudb.manager.handler;

import java.util.ArrayList;
import java.util.List;

import org.opencloudb.config.ErrorCode;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.ManagerParseCheck;
import org.opencloudb.manager.parser.druid.statement.MycatCheckTbStructConsistencyStatement;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;

/**
 * manager端口check语句处理类
 * @author CrazyPig
 * @since 2016-09-08
 *
 */
public class CheckHandler {
	
	public static void handle(String sql, ManagerConnection c, int offset) {
		try {
			SQLStatement stmt = ManagerParseCheck.parse(sql);
			if(stmt instanceof MycatCheckTbStructConsistencyStatement) {
				// TODO 进行表一致性校验逻辑
				MycatCheckTbStructConsistencyStatement _stmt = (MycatCheckTbStructConsistencyStatement) stmt;
				List<String> schemaList = new ArrayList<String>();
				for(SQLExpr sqlExpr : _stmt.getNameList()) {
					schemaList.add(sqlExpr.toString());
				}
				CheckTableStructureConsistencyHandler handler = new CheckTableStructureConsistencyHandler(schemaList.get(0), c);
				handler.handle();
			} else {
				c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement : " + sql);
			}
		} catch(Exception e) {
			e.printStackTrace();
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
		}
	}

}
