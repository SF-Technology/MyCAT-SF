package org.opencloudb.server.handler;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.opencloudb.MycatServer;
import org.opencloudb.cache.CachePool;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.util.StringUtil;

/**
 * 锁表语句处理器
 * 
 * @author songdabin<br>
 *         <a href="http://dev.mysql.com/doc/refman/5.6/en/lock-tables.html">
 *         http://dev.mysql.com/doc/refman/5.6/en/lock-tables.html</a>
 */
public final class LockHandler {

	public static String PARSE_ER_MSG = "You have an error in your SQL syntax; Please check your lock command like 'lock talbes [table] [wirte|read] [,[table] [write|read]]';";

	public static void handle(String sql, int sqlType, ServerConnection c) {

		if (ServerParse.LOCK == sqlType) {
			lockTable(sql, sqlType, c);
		} else if (ServerParse.UNLOCK == sqlType) {
			unlockTable(sql, sqlType, c);
		} else {
			c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, PARSE_ER_MSG);
		}
	}

	private static void lockTable(String sql, int sqlType, ServerConnection c) {
		// 检查当前使用的db
		String schema = c.getSchema();
		if (schema == null) {
			c.writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "No MyCAT Database selected");
			return;
		}
		SchemaConfig schemaConfig = MycatServer.getInstance().getConfig().getSchemas().get(schema);
		Map<String, TableConfig> tableConfigs = schemaConfig.getTables();
		String tsql = StringUtil.replace(sql.trim().toUpperCase(), "`", null);
		String[] words = tsql.split(" ");
		if (words.length >= 4 && "LOCK".equalsIgnoreCase(words[0].trim())
				&& "TABLES".equalsIgnoreCase(words[1].trim())) {
			// lock tables [tableName] [write|read] [,[tableName]
			// [write|read]]语句至少有4个单词，且以lock tables开头
			// 1. 获取sql语句中的table名称以及对应的lock type，并检查表配置
			int a = tsql.toUpperCase().indexOf("TABLES");
			tsql = tsql.substring(a + 6).trim();
			String[] tableAndType = tsql.split(",");
			String[] tables = new String[tableAndType.length];
			String[] types = new String[tableAndType.length];
			for (int i = 0; i < tableAndType.length; i++) {
				String[] table_type = tableAndType[i].split(" ");
				tables[i] = table_type[0];
				types[i] = table_type[1];
				if (tableConfigs.keySet().contains(tables[i])) {
					if (!"WRITE".equalsIgnoreCase(types[i]) && !"READ".equalsIgnoreCase(types[i])) {
						c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, PARSE_ER_MSG);
						return;
					}
				} else {
					c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR,
							"No dataNode found ,please check tables defined in schema:" + schema);
					return;
				}
			}

			// 2. 路由计算
			String cacheKey = schema + sql;
			CachePool sqlRouteCache = MycatServer.getInstance().getCacheService().getCachePool("SQLRouteCache");
			RouteResultset rrs = (RouteResultset) sqlRouteCache.get(cacheKey);
			if (rrs == null) {
				rrs = new RouteResultset(sql, sqlType);
				rrs.setAutocommit(c.isAutocommit());
				Set<String> routeNodes = new HashSet<String>();
				for (int j = 0; j < tables.length; j++) {
					TableConfig tableConf = tableConfigs.get(tables[j]);
					routeNodes.addAll(tableConf.getDataNodes());
				}
				int nodeSize = routeNodes.size();
				Iterator<String> nodeIterator = routeNodes.iterator();
				RouteResultsetNode[] rrsNodes = new RouteResultsetNode[nodeSize];
				for (int k = 0; k < nodeSize; k++) {
					String dataNodeName = nodeIterator.next();
					rrsNodes[k] = new RouteResultsetNode(dataNodeName, sqlType, sql);
				}
				rrs.setNodes(rrsNodes);
				sqlRouteCache.putIfAbsent(cacheKey, rrs);
			}
			c.getSession2().execute(rrs, sqlType);
		} else {
			c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, PARSE_ER_MSG);
		}
	}

	private static void unlockTable(String sql, int sqlType, ServerConnection c) {
		// 检查当前使用的db
		String schema = c.getSchema();
		if (schema == null) {
			c.writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "No MyCAT Database selected");
			return;
		}
		SchemaConfig schemaConfig = MycatServer.getInstance().getConfig().getSchemas().get(schema);

		String tsql = StringUtil.replace(sql.trim().toUpperCase(), "`", null);
		String[] words = tsql.split(" ");
		if (words.length == 2 && "unlock".equalsIgnoreCase(words[0]) && "tables".equalsIgnoreCase(words[1])) {
			String cacheKey = schema + sql;
			CachePool sqlRouteCache = MycatServer.getInstance().getCacheService().getCachePool("SQLRouteCache");
			RouteResultset rrs = (RouteResultset) sqlRouteCache.get(cacheKey);
			if (rrs == null) {
				rrs = new RouteResultset(sql, sqlType);
				rrs.setAutocommit(c.isAutocommit());
				Set<String> routeNodes = schemaConfig.getAllDataNodes();
				int nodeSize = routeNodes.size();
				Iterator<String> nodeIterator = routeNodes.iterator();
				RouteResultsetNode[] rrsNodes = new RouteResultsetNode[nodeSize];
				for (int k = 0; k < nodeSize; k++) {
					String dataNodeName = nodeIterator.next();
					rrsNodes[k] = new RouteResultsetNode(dataNodeName, sqlType, sql);
				}
				rrs.setNodes(rrsNodes);
				sqlRouteCache.putIfAbsent(cacheKey, rrs);
			}
			c.getSession2().execute(rrs, sqlType);
		} else {
			c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, PARSE_ER_MSG);
		}
	}
}
