package org.opencloudb.backend.infoschema;

/**
 * Test MySQLInfoSchemaProcessor
 *
 * @author zagnix
 * @version 1.0
 * @create 2016-09-12 9:30
 */

/**
 *  select * from COLUMNS where TABLE_SCHEMA != 'information_schema' limit 1 \G;
 *************************** 1. row ***************************
 TABLE_CATALOG: def
 TABLE_SCHEMA: exp1
 TABLE_NAME: tl_kafka_log
 COLUMN_NAME: id
 COLUMN_DEFAULT: NULL
 IS_NULLABLE: NO
 DATA_TYPE: varchar
 int CHARACTER_MAXIMUM_LENGTH: 64
 NUMERIC_PRECISION: NULL
 NUMERIC_SCALE: NULL
 DATETIME_PRECISION: NULL
 CHARACTER_SET_NAME: utf8
 COLLATION_NAME: utf8_general_ci
 COLUMN_TYPE: varchar(64)
 COLUMN_KEY: PRI
 EXTRA:NULL
 PRIVILEGES: select,insert,update,references
 */
/**
 *  select * from STATISTICS limit 1\G;
 *************************** 1. row ***************************
 TABLE_CATALOG: def
 TABLE_SCHEMA: exp1
 TABLE_NAME: tl_kafka_log
 NON_UNIQUE: 0
 INDEX_SCHEMA: exp1
 INDEX_NAME: PRIMARY
 SEQ_IN_INDEX: 1
 COLUMN_NAME: id
 COLLATION: A
 CARDINALITY: 0
 SUB_PART: NULL
 PACKED: NULL
 NULLABLE:
 INDEX_TYPE: BTREE
 1 row in set (0.02 sec)
 */

/**
 mysql> desc STATISTICS;
 +---------------+---------------+------+-----+---------+-------+
 | Field         | Type          | Null | Key | Default | Extra |
 +---------------+---------------+------+-----+---------+-------+
 | TABLE_CATALOG | varchar(512)  | NO   |     |         |       |
 | TABLE_SCHEMA  | varchar(64)   | NO   |     |         |       |
 | TABLE_NAME    | varchar(64)   | NO   |     |         |       |
 | NON_UNIQUE    | bigint(1)     | NO   |     | 0       |       |
 | INDEX_SCHEMA  | varchar(64)   | NO   |     |         |       |
 | INDEX_NAME    | varchar(64)   | NO   |     |         |       |
 | SEQ_IN_INDEX  | bigint(2)     | NO   |     | 0       |       |
 | COLUMN_NAME   | varchar(64)   | NO   |     |         |       |
 | COLLATION     | varchar(1)    | YES  |     | NULL    |       |
 | CARDINALITY   | bigint(21)    | YES  |     | NULL    |       |
 | SUB_PART      | bigint(3)     | YES  |     | NULL    |       |
 | PACKED        | varchar(10)   | YES  |     | NULL    |       |
 | NULLABLE      | varchar(3)    | NO   |     |         |       |
 | INDEX_TYPE    | varchar(16)   | NO   |     |         |       |
 | COMMENT       | varchar(16)   | YES  |     | NULL    |       |
 | INDEX_COMMENT | varchar(1024) | NO   |     |         |       |
 +---------------+---------------+------+-----+---------+-------+
 */

/**
 *desc COLUMNS;
 +--------------------------+---------------------+------+-----+---------+-------+
 | Field                    | Type                | Null | Key | Default | Extra |
 +--------------------------+---------------------+------+-----+---------+-------+
 | TABLE_CATALOG            | varchar(512)        | NO   |     |         |       |
 | TABLE_SCHEMA             | varchar(64)         | NO   |     |         |       |
 | TABLE_NAME               | varchar(64)         | NO   |     |         |       |
 | COLUMN_NAME              | varchar(64)         | NO   |     |         |       |
 | ORDINAL_POSITION         | bigint(21) unsigned | NO   |     | 0       |       |
 | COLUMN_DEFAULT           | longtext            | YES  |     | NULL    |       |
 | IS_NULLABLE              | varchar(3)          | NO   |     |         |       |
 | DATA_TYPE                | varchar(64)         | NO   |     |         |       |
 | CHARACTER_MAXIMUM_LENGTH | bigint(21) unsigned | YES  |     | NULL    |       |
 | CHARACTER_OCTET_LENGTH   | bigint(21) unsigned | YES  |     | NULL    |       |
 | NUMERIC_PRECISION        | bigint(21) unsigned | YES  |     | NULL    |       |
 | NUMERIC_SCALE            | bigint(21) unsigned | YES  |     | NULL    |       |
 | DATETIME_PRECISION       | bigint(21) unsigned | YES  |     | NULL    |       |
 | CHARACTER_SET_NAME       | varchar(32)         | YES  |     | NULL    |       |
 | COLLATION_NAME           | varchar(32)         | YES  |     | NULL    |       |
 | COLUMN_TYPE              | longtext            | NO   |     | NULL    |       |
 | COLUMN_KEY               | varchar(3)          | NO   |     |         |       |
 | EXTRA                    | varchar(30)         | NO   |     |         |       |
 | PRIVILEGES               | varchar(80)         | NO   |     |         |       |
 | COLUMN_COMMENT           | varchar(1024)       | NO   |     |         |       |
 +--------------------------+---------------------+------+-----+---------+-------+
 */
/**
 *
 *
 *    private static final String[] MYSQL_INFO_SCHEMA_TCOLUMNS = new String[] {
 "TABLE_SCHEMA",
 "TABLE_NAME",
 "COLUMN_NAME",
 "COLUMN_DEFAULT",
 "IS_NULLABLE",
 "DATA_TYPE",
 "CHARACTER_MAXIMUM_LENGTH",
 "NUMERIC_PRECISION",
 "NUMERIC_SCALE",
 "DATETIME_PRECISION",
 "CHARACTER_SET_NAME",
 "COLLATION_NAME",
 "COLUMN_TYPE",
 "COLUMN_KEY",
 "EXTRA",
 "PRIVILEGES"};

 private static final String[] MYSQL_INFO_SCHEMA_TSTATISTICS = new String[] {
 "TABLE_SCHEMA",
 "TABLE_NAME",
 "INDEX_NAME",
 "SEQ_IN_INDEX",
 "NON_UNIQUE",
 "INDEX_SCHEMA",
 "SEQ_IN_INDEX",
 "COLUMN_NAME",
 "COLLATION",
 "CARDINALITY",
 "SUB_PART",
 "PACKED",
 "NULLABLE",
 "INDEX_TYPE"};
 */

/**
 final String charset = getConfig().getSystem().getCharset();

 final String[] MYSQL_INFO_SCHEMA_TSTATISTICS = new String[] {
 "TABLE_SCHEMA",
 "TABLE_NAME",
 "INDEX_NAME",
 "INDEX_SCHEMA",
 "COLUMN_NAME",
 "CARDINALITY"
 };

 Map<String, PhysicalDBPool> nodes = config.getDataHosts();
 MySQLInfoSchemaProcessor processor = null;

 String execSQL = "select ";

 for (String colname: MYSQL_INFO_SCHEMA_TSTATISTICS) {
 execSQL +=colname + ",";
 }

 execSQL +="CARDINALITY from STATISTICS where TABLE_SCHEMA != 'information_schema'";

 try {
 processor = new MySQLInfoSchemaProcessor("information_schema", nodes.size(),
 execSQL, MYSQL_INFO_SCHEMA_TSTATISTICS,
 new SQLQueryResultListener<HashMap<String,LinkedList<byte[]>>>() {
    @Override
    public void onResult(HashMap<String,LinkedList<byte[]>> result ) {
      //TODO handle result
    }
});
 processor.processSQL();
 } catch (Exception e) {
 e.printStackTrace();
 }
 }
 */