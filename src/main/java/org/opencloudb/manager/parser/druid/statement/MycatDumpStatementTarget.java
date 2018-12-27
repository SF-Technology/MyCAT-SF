package org.opencloudb.manager.parser.druid.statement;

/**
 * 
 * @author 01169238
 * @since 1.5.3
 *
 */
public enum MycatDumpStatementTarget {
	ALL // dump all
	, ALL_TABLES // dump all tables
	, SCHEMAS // dump special schemas
	, TABLES // dump special tables
}
