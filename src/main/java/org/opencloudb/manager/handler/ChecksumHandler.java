package org.opencloudb.manager.handler;

import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.MycatManageStatementParser;
import org.opencloudb.manager.parser.druid.statement.MycatChecksumTableStatement;
import org.opencloudb.util.StringUtil;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;

/**
 * manager端口checksum语句处理类
 * @author CrazyPig
 * @since 2017-01-13
 *
 */
public class ChecksumHandler {
	
	public static void handle(String sql, ManagerConnection c) {
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		try {
			SQLStatement stmt = parser.parseStatement();
			if(stmt instanceof MycatChecksumTableStatement) {
				MycatChecksumTableStatement _stmt = (MycatChecksumTableStatement) stmt;
				if(_stmt.getTableName() instanceof SQLIdentifierExpr) {
					// 没有提供schema
					c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "please provide schema! the sql grammar is : checksum table schema.tablename");
					return ;
				}
				SQLPropertyExpr tableNameExpr = (SQLPropertyExpr) _stmt.getTableName();
				if(tableNameExpr.getOwner() == null || StringUtil.isEmpty(tableNameExpr.getOwner().toString())) {
					// 没有提供schema
					c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "please provide schema! the sql grammar is : checksum table schema.tablename");
					return ;
				}
				String schemaName = tableNameExpr.getOwner().toString();
				SchemaConfig schemaConf = MycatServer.getInstance().getConfig().getSchemas().get(schemaName);
				if(schemaConf == null) {
					// 找不到对应schema
					c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "no schema named '" + schemaName + "' defined in schema.xml!");
					return ;
				}
				String tableName = tableNameExpr.getSimpleName();
				if(StringUtil.isEmpty(tableName)) {
					// 没有提供table name
					c.writeErrMessage(ErrorCode.ER_UNKNOWN_TABLE, "please provide tablename! the sql grammar is : checksum table schema.tablename");
					return ;
				}
				TableConfig tableConf = schemaConf.getTables().get(tableName.toUpperCase());
				if(tableConf == null) {
					// 找不到对应的表
					c.writeErrMessage(ErrorCode.ER_UNKNOWN_TABLE, "no table named '" + tableName + "' defined in schema '" + schemaName + "'");
					return ;
				}
				if(!tableConf.isGlobalTable()) {
					// 非全局表
					c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "This command can only use to checksum global table!");
					return ;
				}
				ChecksumTableHandler handler = new ChecksumTableHandler();
				handler.handle(schemaName, tableConf, c);
			} else {
				c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement : " + sql);
			}
		} catch(Exception e) {
			e.printStackTrace();
			c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
		}
	}

}
