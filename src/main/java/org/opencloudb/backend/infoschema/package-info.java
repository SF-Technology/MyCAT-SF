package org.opencloudb.backend.infoschema;

/**
 * Test MySQLInfoSchemaProcessor
 *
 * @author zagnix
 * @version 1.0
 * @create 2016-09-12 9:30
 */

/**
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

        try {
        processor = new MySQLInfoSchemaProcessor("information_schema",nodes.size(),execSQL,MYSQL_INFO_SCHEMA_TCOLUMNS);
        } catch (Exception e) {
        e.printStackTrace();
        }
        try {
        HashMap<String,LinkedList<byte[]>> mapIterator =  processor.processSQL();
        for (String key:mapIterator.keySet()) {
        LOGGER.error("key :" + key + ", linked size :" + mapIterator.get(key).size());
        }

        } catch (Exception e) {
        e.printStackTrace();
        }

 */