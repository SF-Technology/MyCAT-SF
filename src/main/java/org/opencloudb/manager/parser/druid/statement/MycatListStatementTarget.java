package org.opencloudb.manager.parser.druid.statement;

public enum MycatListStatementTarget {
	SCHEMAS // list schemas
	, TABLES // list tables
	, DATANODES // list datanodes
	, DATAHOSTS // list datahosts
	, RULES // list rules
	, FUNCTIONS // list functions
	, USERS // list users
	, SYSTEM_VARIABLES // system variables
	, SQLWALL_VARIABLES // sqlwall variables
	, MAPFILES // mapfiles
	, BACKUPS // backups, 配置信息的备份文件
}
