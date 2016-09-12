package org.opencloudb.backend.infoschema;

/**
 * Test MySQLInfoSchemaProcessor
 *
 * @author zagnix
 * @version 1.0
 * @create 2016-09-12 9:30
 */

/**
 *
 *
 final String[] MYSQL_INFO_SCHEMA_TCOLUMNS = new String[] {
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


 Map<String, PhysicalDBPool> nodes = config
 .getDataHosts();

 MySQLInfoSchemaProcessor processor =
 null;

 String execSQL = "select  ";

 for (String colname: MYSQL_INFO_SCHEMA_TCOLUMNS
 ) {
 execSQL +=colname + ",";
 }

 execSQL +="PRIVILEGES from COLUMNS where TABLE_SCHEMA != 'information_schema'";
 information_schema 一定要保持跟MySQL的db名一直。
 try {
 processor = new MySQLInfoSchemaProcessor("information_schema",nodes.size(),execSQL,MYSQL_INFO_SCHEMA_TCOLUMNS);
 } catch (IOException e) {
 e.printStackTrace();
 }
 try {
 Iterator<UnsafeRow> iterator=  processor.processSQL();

 int count = 0;

 while (iterator.hasNext()){
 count++;
 UnsafeRow row = iterator.next();
 }

 LOGGER.error("count:" + count);

 } catch (Exception e) {
 e.printStackTrace();
 }


 */