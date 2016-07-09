package org.opencloudb.parser.druid.impl;

import java.sql.SQLNonTransientException;
import java.util.List;

import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.parser.druid.DruidParser;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.parser.ServerParse;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLockTableStatement;

public class DruidLockTableStatement extends DefaultDruidParser implements DruidParser {

	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt)
			throws SQLNonTransientException {
		MySqlLockTableStatement lockTableStat = (MySqlLockTableStatement)stmt;
		String table = lockTableStat.getTableSource().toString().toUpperCase();
		TableConfig tableConfig = schema.getTables().get(table);
		List<String> dataNodes = tableConfig.getDataNodes();
		RouteResultsetNode[] nodes = new RouteResultsetNode[dataNodes.size()];
		for (int i = 0; i < dataNodes.size(); i ++) {
			nodes[i] = new RouteResultsetNode(dataNodes.get(i), ServerParse.LOCK, stmt.toString());
		}
		rrs.setNodes(nodes);
		rrs.setFinishedRoute(true);
	}
}
