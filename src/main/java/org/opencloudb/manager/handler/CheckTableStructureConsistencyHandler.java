package org.opencloudb.manager.handler;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.opencloudb.MycatServer;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.Fields;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.handler.CheckTableStructureConsistencyHandler.TableCheckResult.AddItem;
import org.opencloudb.manager.handler.CheckTableStructureConsistencyHandler.TableCheckResult.DeleteItem;
import org.opencloudb.manager.handler.CheckTableStructureConsistencyHandler.TableCheckResult.DiffItem;
import org.opencloudb.manager.handler.CheckTableStructureConsistencyHandler.TableColumns.Column;
import org.opencloudb.manager.handler.CheckTableStructureConsistencyHandler.TableIndexes.Index;
import org.opencloudb.monitor.CheckTableStructureConsistencyInfo;
import org.opencloudb.mysql.MySQLMessage;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.FrontendConnection;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.util.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

/**
 * 分片表以及全局表表结构一致性校验逻辑处理类
 * 
 * @author CrazyPig
 * @since 2016-09-08
 *
 */
public class CheckTableStructureConsistencyHandler {
	private static final Logger LOGGER = LoggerFactory.getLogger(CheckTableStructureConsistencyHandler.class);
	private static final int FIELD_COUNT = 1; // 返回结果集列数量
	private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	private boolean isManagerConnection = true;
	/*
	 *  定义返回结果集header和field
	 */
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;

		fields[i] = PacketUtil.getField("CHECK_RESULT", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		eof.packetId = ++packetId;
	}
	
	/**
	 * INFORMATION_SCHEMA.COLUMNS 定义拉取的列名 
	 */
	private static final String[] MYSQL_INFO_SCHEMA_TCOLUMNS = new String[] {
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "COLUMN_NAME",
            "COLUMN_TYPE",
            "IS_NULLABLE",
            "COLUMN_DEFAULT",
//            "DATA_TYPE",
//            "CHARACTER_MAXIMUM_LENGTH",
//            "NUMERIC_PRECISION",
//            "NUMERIC_SCALE",
//            "DATETIME_PRECISION",
//            "CHARACTER_SET_NAME",
//            "COLLATION_NAME",
//            "COLUMN_KEY",
            "EXTRA",
//            "PRIVILEGES"
    };
	
	/**
	 * INFORMATION_SCHEMA.STATISTICS 定义拉取的列名
	 */
	private static final String[] MYSQL_INFO_SCHEMA_TSTATISTICS = new String[] {
			 "TABLE_SCHEMA",
			 "TABLE_NAME",
			 "COLUMN_NAME",
			 "INDEX_NAME",
			 "SEQ_IN_INDEX",
//			 "NON_UNIQUE",
//			 "INDEX_SCHEMA",
//			 "SEQ_IN_INDEX",
			 "COLLATION",
//			 "CARDINALITY",
//			 "SUB_PART",
//			 "PACKED",
//			 "NULLABLE",
			 "INDEX_TYPE"
	};
	
	private static final int COL_IDX_OF_TABLE_SCHEMA = 0; // TABLE_SCHEMA列值索引
	private static final int COL_IDX_OF_TABLE_NAME = 1; // TABLE_NAME列值索引
	private static final int COL_IDX_OF_COLUMN_NAME = 2; // COLUMN_NAME列值索引
	private static final int COL_IDX_OF_COLUMN_TYPE = 3;
	private static final int COL_IDX_OF_IS_NULLABLE = 4;
	private static final int COL_IDX_OF_COLUMN_DEFAULT = 5;
	private static final int COL_IDX_OF_EXTRA = 6;
	
	private static final int COL_IDX_OF_INDEX_NAME = 3;
	private static final int COL_IDX_OF_SEQ_IN_INDEX = 4;
	private static final int COL_IDX_OF_COLLATION = 5;
	private static final int COL_IDX_OF_INDEX_TYPE = 6;
	
	private static final String SPLIT_STR = "$";
	private static final String LINE_SEP = System.getProperty("line.separator");
	
	private ManagerConnection source;
	private String schemaName;
	
	private String sql1;
	private String sql2;
	private CheckTableColDefConsistencyHandler handler1;
	private CheckTableIdxDefConsistencyHandler handler2;
	private Map<String, String> dhDbToDnMap;
	private Map<String, TableCheckResult> resultMap;
	
	/**
	 * 封装表一致性校验结果
	 * @author CrazyPig
	 *
	 */
	static class TableCheckResult {
		
		String tableName;
		// 列定义交集,新增,缺失,差异项
		Set<String> commColNameSet = new TreeSet<String>();
		List<AddItem> addColItems = new ArrayList<AddItem>();
		List<DeleteItem> delColItems = new ArrayList<DeleteItem>();
		List<DiffItem> diffColItems = new ArrayList<DiffItem>();
		// 索引定义交集,新增,缺失,差异项
		Set<String> commIdxNameSet = new TreeSet<String>();
		List<AddItem> addIdxItems = new ArrayList<AddItem>();
		List<DeleteItem> delIdxItems = new ArrayList<DeleteItem>();
		List<DiffItem> diffIdxItems = new ArrayList<DiffItem>();
		
		public boolean isColDefConsistent() {
			int size = 0;
			size += addColItems.size();
			size += delColItems.size();
			size += diffColItems.size();
			return size == 0;
		}
		
		public boolean isIdxDefConsistent() {
			int size = 0;
			size += addIdxItems.size();
			size += delIdxItems.size();
			size += diffIdxItems.size();
			return size == 0;
		}
		
		public boolean isConsistent() {
			int size = 0;
			size += addColItems.size();
			size += delColItems.size();
			size += diffColItems.size();
			size += addIdxItems.size();
			size += delIdxItems.size();
			size += diffIdxItems.size();
			return size == 0;
		}
		
		static class AddItem {
			
			String dn;
			String db;
			String key;
			String value;
			
			public AddItem(String dataNode, String database, String key, String value) {
				this.dn = dataNode;
				this.db = database;
				this.key = key;
				this.value = value;
			}
			
		}
		
		static class DeleteItem {
			
			String dn;
			String db;
			String key;
			String value;
			
			public DeleteItem(String dataNode, String database, String key, String value) {
				this.dn = dataNode;
				this.db = database;
				this.key = key;
				this.value = value;
			}
			
		}
		
		static class DiffItem {
			
			String key;
			Map<String, Set<String>> diffGroup;
			
			public DiffItem(String key, Map<String, Set<String>> diffGroup) {
				this.key = key;
				this.diffGroup = diffGroup;
			}
			
		}
		
	}
	
	/**
	 * 封装从INFORMATION_SCHEMA.COLUMNS传递过来的数据
	 * @author CrazyPig
	 * @since 2016-09-12
	 *
	 */
	static class TableColumns {
		
		String dataNode; // 所在dn
		String tableSchema; // 真实schema
		String tableName; // 表名
		Map<String, Column> columns = new HashMap<String, Column>(); // 列信息
		
		public TableColumns(String dataNode, String tableSchema, String tableName) {
			this.dataNode = dataNode;
			this.tableSchema = tableSchema;
			this.tableName = tableName;
		}
		
		static class Column {
			String columnName;
			Map<String, String> props = new HashMap<String, String>();
			
			public Column(String columnName, Map<String, String> props) {
				this.columnName = columnName;
				this.props = props;
			}
			
			public String getColumnDefine() {
				String columnType = props.get(MYSQL_INFO_SCHEMA_TCOLUMNS[COL_IDX_OF_COLUMN_TYPE]);
				String isNullable = props.get(MYSQL_INFO_SCHEMA_TCOLUMNS[COL_IDX_OF_IS_NULLABLE]);
				String columnDefault = props.get(MYSQL_INFO_SCHEMA_TCOLUMNS[COL_IDX_OF_COLUMN_DEFAULT]);
				String extra = props.get(MYSQL_INFO_SCHEMA_TCOLUMNS[COL_IDX_OF_EXTRA]);
				StringBuilder sb = new StringBuilder();
				sb.append(columnType);
				if("NO".equalsIgnoreCase(isNullable)) {
					sb.append(" NOT NULL");
				} else {
					sb.append(" DEFAULT " + columnDefault);
				}
				if(!"".equals(extra) && extra != null) {
					sb.append(" " + extra.toUpperCase());
				}
				return sb.toString();
			}
			
		}
		
		void addColumn(String column, Map<String, String> props) {
			this.columns.put(column, new Column(column, props));
		}
		
	}
	
	/**
	 * 封装从INFORMATION_SCHEMA.STATISTICS传递过来的数据
	 * @author CrazyPig
	 * @since 2016-09-19
	 *
	 */
	static class TableIndexes {
		
		String dataNode; // 所在dn
		String tableSchema; // 真实schema
		String tableName; // 表名
		Map<String, Index> indexes = new HashMap<String, Index>(); // 索引信息
		
		public TableIndexes(String dataNode, String tableSchema, String tableName) {
			this.dataNode = dataNode;
			this.tableSchema = tableSchema;
			this.tableName = tableName;
		}
		
		static class Index {
			
			String indexName; // 索引名
			
			/*
			 * key = SEQ_IN_INDEX 表示复合索引列的位置
			 */
			Map<Integer, Map<String, String>> props = new HashMap<Integer, Map<String, String>>();
			
			public Index(String indexName, Map<Integer, Map<String, String>> props) {
				this.indexName = indexName;
				this.props = props;
			}
			
			public void put(Integer seqInIndex, Map<String, String> subProps) {
				this.props.put(seqInIndex, subProps);
			}
			
			public String getIndexDefine() {
				StringBuilder sb = new StringBuilder();
				String indexType = null;
				String indexCollation = null;
				if("PRIMARY".equals(this.indexName)) {
					sb.append("PRIMARY KEY (");
				} else {
					sb.append("KEY (");
				}
				int size = this.props.size();
				for(int i = 1; i <= size; i++) {
					Map<String, String> subProps = this.props.get(i);
					String columnName = subProps.get(MYSQL_INFO_SCHEMA_TSTATISTICS[COL_IDX_OF_COLUMN_NAME]);
					if(i == size) {
						indexType = subProps.get(MYSQL_INFO_SCHEMA_TSTATISTICS[COL_IDX_OF_INDEX_TYPE]);
						indexCollation = subProps.get(MYSQL_INFO_SCHEMA_TSTATISTICS[COL_IDX_OF_COLLATION]);
						sb.append("'" + columnName + "'");
					} else {
						sb.append("'" + columnName + "',");
					}
				}
				if("A".equalsIgnoreCase(indexCollation)) {
					indexCollation = "ASC";
				} else {
					indexCollation = "DESC";
				}
				sb.append(") " + indexCollation + " USING " + indexType);
				return sb.toString();
			}
			
		}
		
		void addIndex(String indexName, Integer seqInIndex, Map<String, String> subProps) {
			Index index = indexes.get(indexName);
			if(index == null) {
				Map<Integer, Map<String, String>> props = new HashMap<Integer, Map<String, String>>();
				index = new Index(indexName, props);
				indexes.put(indexName, index);
			}
			index.put(seqInIndex, subProps);
		}
		
	}
	
	/**
	 * 获取表列定义的handler
	 * @author CrazyPig
	 * @since 2016-09-19
	 *
	 */
	class CheckTableColDefConsistencyHandler extends SQLQueryResultHandlerChain<HashMap<String, LinkedList<byte[]>>> {

		public CheckTableColDefConsistencyHandler(FrontendConnection frontend, String schema, String sql) {
			super(frontend, schema, sql);
		}

		@Override
		public void processResult(HashMap<String, LinkedList<byte[]>> mapHostData) {
			ManagerConnection c = null;
			String charset = "utf8";
			if (isManagerConnection) {
				 c = CheckTableStructureConsistencyHandler.this.source;
				 charset = c.getCharset();
			}

			Map<String, Map<String, TableColumns>> tableColumnsMap = new HashMap<String, Map<String, TableColumns>>();
			int colSize = MYSQL_INFO_SCHEMA_TCOLUMNS.length;
			int colIdxOfTableSchema = COL_IDX_OF_TABLE_SCHEMA;
			int colIdxOfTableName = COL_IDX_OF_TABLE_NAME;
			int colIdxOfColumnName = COL_IDX_OF_COLUMN_NAME;
			
			for(String dataNode : mapHostData.keySet()) {
				LinkedList<byte[]> packs = mapHostData.get(dataNode);
				byte[] data = null;
				while((data = packs.poll()) != null) {
					// 读取mysql包数据
					MySQLMessage mm = new MySQLMessage(data);
					mm.readUB3();
		            mm.read();
		            List<byte[]> colValueList = new ArrayList<byte[]>();
		            // 获取列字节数据
		            for(int i = 0; i < colSize; i++) {
		            	byte[] colValue = mm.readBytesWithLength();
		            	colValueList.add(colValue);
		            }
		            
					String tableName = ByteUtil.getString(colValueList.get(colIdxOfTableName), charset);
					if(tableColumnsMap.get(tableName) == null) {
						tableColumnsMap.put(tableName, new HashMap<String, TableColumns>());
					}
					String tableSchema = ByteUtil.getString(colValueList.get(colIdxOfTableSchema)); // 得到table schema
					String key = dataNode + SPLIT_STR + tableSchema; // 得到key, key = dnName_tbSchema
					if(tableColumnsMap.get(tableName).get(key) == null) {
						tableColumnsMap.get(tableName).put(key, new TableColumns(dataNode, tableSchema, tableName));
					}
					// 存放记录
					String columnName = ByteUtil.getString(colValueList.get(colIdxOfColumnName)); // 得到column name
					Map<String, String> valueMap = new HashMap<String, String>();
					for(int i = 0; i < colSize; i++) {
						if(i == colIdxOfTableSchema || i == colIdxOfTableName || i == colIdxOfColumnName) {
							continue;
						}
						String colKey = MYSQL_INFO_SCHEMA_TCOLUMNS[i]; // 得到key, 该key是每行记录的列名
						byte[] bytesVal = colValueList.get(i);
						String value = null;
						if(bytesVal != null) {
							value = ByteUtil.getString(colValueList.get(i), charset);
						}
						valueMap.put(colKey, value);
					}
					tableColumnsMap.get(tableName).get(key).addColumn(columnName, valueMap);
				}
			}
			
			// 初始化结果集, 每个表对应一个结果
			resultMap = new HashMap<String, TableCheckResult>();
			for (String tableName : tableColumnsMap.keySet()) {
				resultMap.put(tableName, new TableCheckResult());
			}
			
			// 找出每个表共同存在的列(交集,以交集为基准)
			Map<String, Set<String>> tableCommColSetMap = new HashMap<String, Set<String>>();
			for(String tableName : tableColumnsMap.keySet()) {
				Map<String, TableColumns> colNameSetMap = tableColumnsMap.get(tableName);
				Set<String> commColNameSet = new TreeSet<String>();
				int count = 0;
				for(String key : colNameSetMap.keySet()) {
					TableColumns tableColumns = colNameSetMap.get(key);
					if(count == 0) {
						commColNameSet.addAll(tableColumns.columns.keySet());
					} else {
						commColNameSet.retainAll(tableColumns.columns.keySet());
					}
					count++;
				}
				tableCommColSetMap.put(tableName, commColNameSet);
				resultMap.get(tableName).commColNameSet.addAll(commColNameSet);
			}
			
			checkAddOrDelete(tableColumnsMap, tableCommColSetMap);
			
			checkDiff(tableColumnsMap, tableCommColSetMap);
			
			// 回收资源
			tableColumnsMap.clear();
			tableColumnsMap = null;
		}
		
		private void checkAddOrDelete(Map<String, Map<String, TableColumns>> tableColumnsMap,
				Map<String, Set<String>> tableCommColSetMap) {
			
			for(String tableName : tableColumnsMap.keySet()) {
				Set<String> commColNameSet = tableCommColSetMap.get(tableName);
				// key = dnName_dbName
				Map<String, TableColumns> columnsMap = tableColumnsMap.get(tableName);
				for(String key : columnsMap.keySet()) {
					TableColumns tableColumns = columnsMap.get(key);
					if(tableColumns.columns.keySet().size() > commColNameSet.size()) { // 有新增的列
						// 记录新增的列
						Set<String> addColNameSet = new TreeSet<String>(tableColumns.columns.keySet());
						addColNameSet.removeAll(commColNameSet);
						String[] dnAndDb = unCombineKey(SPLIT_STR, key);
						String dn = dnAndDb[0];
						String db = dnAndDb[1];
						for(String addColName : addColNameSet) {
							Column column = tableColumns.columns.get(addColName);
							String columnDefine = column.getColumnDefine();
							resultMap.get(tableName).addColItems.add(new AddItem(dn, db, addColName, columnDefine));
						}
						
					} else if(tableColumns.columns.keySet().size() < commColNameSet.size()) { // 缺失了列
						// 记录缺失的列
						Set<String> delColNameSet = new TreeSet<String>(commColNameSet);
						delColNameSet.removeAll(tableColumns.columns.keySet());
						String[] dnAndDb = unCombineKey(SPLIT_STR, key);
						String dn = dnAndDb[0];
						String db = dnAndDb[1];
						for(String delColName : delColNameSet) {
							Column column = tableColumns.columns.get(delColName);
							String columnDefine = column.getColumnDefine();
							resultMap.get(tableName).delColItems.add(new DeleteItem(dn, db, delColName, columnDefine));
						}
					}
				}
			}
		}
		
		private void checkDiff(Map<String, Map<String, TableColumns>> tableColumnsMap, 
				Map<String, Set<String>> tableCommColSetMap) {
			
			for(String tableName : tableColumnsMap.keySet()) {
				Map<String, TableColumns> secondTableColumnsMap = tableColumnsMap.get(tableName);
				Map<String, Map<String, Set<String>>> tmpMap = new HashMap<String, Map<String, Set<String>>>(); 
				for(String key : secondTableColumnsMap.keySet()) {
					TableColumns tableColumns = secondTableColumnsMap.get(key);
					Set<String> tableCommColSet = tableCommColSetMap.get(tableName);
					for(String columnName : tableColumns.columns.keySet()) {
						if(!tableCommColSet.contains(columnName)) {
							continue;
						}
						Column column = tableColumns.columns.get(columnName);
						String combineKey = getCombineKey(SPLIT_STR, tableName, columnName);
						String value = column.getColumnDefine();
						if(tmpMap.get(combineKey) == null) {
							tmpMap.put(combineKey, new HashMap<String, Set<String>>());
						}
						if(tmpMap.get(combineKey).get(value) == null) {
							tmpMap.get(combineKey).put(value, new TreeSet<String>());
						}
						tmpMap.get(combineKey).get(value).add(key);
					}
				}
				
				// 检查结果就包含在tmpMap中
				for(String combineKey : tmpMap.keySet()) {
					Map<String, Set<String>> valueGroupOfItem = tmpMap.get(combineKey);
					if(valueGroupOfItem.size() > 1) { // 列定义不一致的情况
						String[] tbAndCol = unCombineKey(SPLIT_STR, combineKey);
						String columnName = tbAndCol[1];
						resultMap.get(tableName).diffColItems.add(new DiffItem(columnName, valueGroupOfItem));
					}
				}
				
				// 回收资源
				tmpMap.clear();
				tmpMap = null;
				
			}
		}

	}
	
	/**
	 * 定义获取表索引定义的handler
	 * @author CrazyPig
	 * @since 2016-09-19
	 *
	 */
	class CheckTableIdxDefConsistencyHandler extends SQLQueryResultHandlerChain<HashMap<String, LinkedList<byte[]>>> {
		
		public CheckTableIdxDefConsistencyHandler(FrontendConnection frontend, String schema, String sql) {
			super(frontend, schema, sql);
		}

		@Override
		public void processResult(HashMap<String, LinkedList<byte[]>> mapHostData) {
			
			ManagerConnection c = CheckTableStructureConsistencyHandler.this.source;
			String charset = null;
			if (c != null)
				charset= c.getCharset();
			else
				charset  = "utf8";
			
			int colSize = MYSQL_INFO_SCHEMA_TSTATISTICS.length;
			int colIdxOfTableSchema = COL_IDX_OF_TABLE_SCHEMA;
			int colIdxOfTableName = COL_IDX_OF_TABLE_NAME;
			int colIdxOfIndexName = COL_IDX_OF_INDEX_NAME;
			int colIdxOfSeqInIndex = COL_IDX_OF_SEQ_IN_INDEX;
			
			Map<String, Map<String, TableIndexes>> tableIndexesMap = new HashMap<String, Map<String, TableIndexes>>();
			
			for(String dataNode : mapHostData.keySet()) {
				LinkedList<byte[]> packs = mapHostData.get(dataNode);
				byte[] data = null;
				while((data = packs.poll()) != null) {
					// 读取mysql包数据
					MySQLMessage mm = new MySQLMessage(data);
					mm.readUB3();
		            mm.read();
		            List<byte[]> colValueList = new ArrayList<byte[]>();
		            // 获取列字节数据
		            for(int i = 0; i < colSize; i++) {
		            	byte[] colValue = mm.readBytesWithLength();
		            	colValueList.add(colValue);
		            }
		            
		            String tableName = ByteUtil.getString(colValueList.get(colIdxOfTableName), charset);
		            if(tableIndexesMap.get(tableName) == null) {
		            	tableIndexesMap.put(tableName, new HashMap<String, TableIndexes>());
		            }
		            String tableSchema = ByteUtil.getString(colValueList.get(colIdxOfTableSchema)); // 得到table schema
					String key = dataNode + SPLIT_STR + tableSchema; // 得到key, key = dnName_tbSchema
					if(tableIndexesMap.get(tableName).get(key) == null) {
						tableIndexesMap.get(tableName).put(key, new TableIndexes(dataNode, tableSchema, tableName));
					}
					// 存放记录
					String indexName = ByteUtil.getString(colValueList.get(colIdxOfIndexName), charset);
					Integer seqInIndex = ByteUtil.getInt(colValueList.get(colIdxOfSeqInIndex));
					Map<String, String> subProps = new HashMap<String, String>();
					for(int i = 0; i < colSize; i++) {
						if(i == colIdxOfTableSchema || i == colIdxOfTableName || i == colIdxOfIndexName || i == colIdxOfSeqInIndex) {
							continue;
						}
						String colKey = MYSQL_INFO_SCHEMA_TSTATISTICS[i]; // 得到key, 该key是每行记录的列名
						byte[] bytesVal = colValueList.get(i);
						String value = null;
						if(bytesVal != null) {
							value = ByteUtil.getString(colValueList.get(i), charset);
						}
						subProps.put(colKey, value);
					}
					tableIndexesMap.get(tableName).get(key).addIndex(indexName, seqInIndex, subProps);
				}
			}
			
			// 找出共同有的index
			Map<String, Set<String>> tableCommIdxSetMap = new HashMap<String, Set<String>>();
			for(String tableName : tableIndexesMap.keySet()) {
				Map<String, TableIndexes> IdxNameSetMap = tableIndexesMap.get(tableName);
				Set<String> commIdxNameSet = new TreeSet<String>();
				int count = 0;
				for(String key : IdxNameSetMap.keySet()) {
					TableIndexes tableIndexes = IdxNameSetMap.get(key);
					if(count == 0) {
						commIdxNameSet.addAll(tableIndexes.indexes.keySet());
					} else {
						commIdxNameSet.retainAll(tableIndexes.indexes.keySet());
					}
					count++;
				}
				tableCommIdxSetMap.put(tableName, commIdxNameSet);
				resultMap.get(tableName).commIdxNameSet.addAll(commIdxNameSet);
			}
			
			// 比较新增和缺失
			checkAddOrDelete(tableIndexesMap, tableCommIdxSetMap);
			// 比较差异
			checkDiff(tableIndexesMap, tableCommIdxSetMap);
			
			// 回收资源
			tableIndexesMap.clear();
			tableIndexesMap = null;
			
			// 处理响应
			try {
				response(resultMap);
			} catch (Exception e) {
				if (c != null) {
					c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
				}else {
					LOGGER.error(e.getMessage());
				}
			}
			
		}
		
		private void checkAddOrDelete(Map<String, Map<String, TableIndexes>> tableIndexesMap, 
				Map<String, Set<String>> tableCommIdxSetMap) {
			
			for(String tableName : tableIndexesMap.keySet()) {
				Set<String> commIdxNameSet = tableCommIdxSetMap.get(tableName);
				// key = dnName_dbName
				Map<String, TableIndexes> indexesMap = tableIndexesMap.get(tableName);
				for(String key : indexesMap.keySet()) {
					TableIndexes tableIndexes = indexesMap.get(key);
					if(tableIndexes.indexes.keySet().size() > commIdxNameSet.size()) { // 有新增的索引
						// 记录新增的索引
						Set<String> addIdxNameSet = new TreeSet<String>(tableIndexes.indexes.keySet());
						addIdxNameSet.removeAll(commIdxNameSet);
						String[] dnAndDb = unCombineKey(SPLIT_STR, key);
						String dn = dnAndDb[0];
						String db = dnAndDb[1];
						for(String addIdxName : addIdxNameSet) {
							Index index = tableIndexes.indexes.get(addIdxName);
							String indexDefine = index.getIndexDefine();
							resultMap.get(tableName).addIdxItems.add(new AddItem(dn, db, addIdxName, indexDefine));
						}
						
					} else if(tableIndexes.indexes.keySet().size() < commIdxNameSet.size()) { // 缺失了索引
						// 记录缺失的索引
						Set<String> delIdxNameSet = new TreeSet<String>(commIdxNameSet);
						delIdxNameSet.removeAll(tableIndexes.indexes.keySet());
						String[] dnAndDb = unCombineKey(SPLIT_STR, key);
						String dn = dnAndDb[0];
						String db = dnAndDb[1];
						for(String delIdxName : delIdxNameSet) {
							Index index = tableIndexes.indexes.get(delIdxName);
							String indexDefine = index.getIndexDefine();
							resultMap.get(tableName).delIdxItems.add(new DeleteItem(dn, db, delIdxName, indexDefine));
						}
					}
				}
			}
			
		}
		
		private void checkDiff(Map<String, Map<String, TableIndexes>> tableIndexesMap, 
				Map<String, Set<String>> tableCommIdxSetMap) {
			
			for(String tableName : tableIndexesMap.keySet()) {
				Map<String, TableIndexes> secondTableIndexesMap = tableIndexesMap.get(tableName);
				Map<String, Map<String, Set<String>>> tmpMap = new HashMap<String, Map<String, Set<String>>>(); 
				for(String key : secondTableIndexesMap.keySet()) {
					TableIndexes tableIndexes = secondTableIndexesMap.get(key);
					Set<String> tableCommIdxSet = tableCommIdxSetMap.get(tableName);
					for(String indexName : tableIndexes.indexes.keySet()) {
						if(!tableCommIdxSet.contains(indexName)) {
							continue;
						}
						Index index = tableIndexes.indexes.get(indexName);
						String combineKey = getCombineKey(SPLIT_STR, tableName, indexName);
						String value = index.getIndexDefine();
						if(tmpMap.get(combineKey) == null) {
							tmpMap.put(combineKey, new HashMap<String, Set<String>>());
						}
						if(tmpMap.get(combineKey).get(value) == null) {
							tmpMap.get(combineKey).put(value, new TreeSet<String>());
						}
						tmpMap.get(combineKey).get(value).add(key);
					}
				}
				
				// 检查结果就包含在tmpMap中
				for(String combineKey : tmpMap.keySet()) {
					Map<String, Set<String>> valueGroupOfItem = tmpMap.get(combineKey);
					if(valueGroupOfItem.size() > 1) { // 索引定义不一致的情况
						String[] tbAndIdx = unCombineKey(SPLIT_STR, combineKey);
						String indexName = tbAndIdx[1];
						resultMap.get(tableName).diffIdxItems.add(new DiffItem(indexName, valueGroupOfItem));
					}
				}
				
				// 回收资源
				tmpMap.clear();
				tmpMap = null;
				
			}
		}
		
	}

	/**
	 *
	 * @param schemaName
	 * @param c
	 * @param isManagerConnection true表示是mycat管理端口发送check命令，false表示定时check命令
	 */
	
	public CheckTableStructureConsistencyHandler(String schemaName, ManagerConnection c,boolean isManagerConnection) {
		this.schemaName = schemaName;
		this.source = c;
		this.isManagerConnection = isManagerConnection;
	}


	public void handle() throws Exception {
		if (isManagerConnection){
			handleWithManagerConnection();
		}else {
			handleNotWithManagerConnection();
		}
	}

	public void handleNotWithManagerConnection() throws Exception{

		Map<String, SchemaConfig> schemaMap = MycatServer.getInstance().getConfig().getSchemas();
		// 1. 获取选择的schema
		SchemaConfig selSchemaCfg = schemaMap.get(this.schemaName);
		if(selSchemaCfg == null) {
			LOGGER.error("can not find schema [ " + this.schemaName + " ] in mycat schema.xml");
			return ;
		}
		// 2. 收集对应的分片表和全局表,待下一步处理
		List<TableConfig> tableCfgList = new ArrayList<TableConfig>();
		Set<String> tableSet = new TreeSet<String>();
		for(String tableName : selSchemaCfg.getTables().keySet()) {
			TableConfig tableCfg = selSchemaCfg.getTables().get(tableName);
			if(tableCfg.getRule() != null || tableCfg.isGlobalTable()) {
				tableCfgList.add(tableCfg);
				tableSet.add("'" + tableName + "'");
			}
		}
		// 如果选择的schema没有分片表或者全局表, 返回
		if (tableSet.size() <= 0) {
            return ;
        }

		Set<String> realSchemaSet = getRealSchemaSet(tableCfgList);
		sql1 = "select " + Joiner.on(",").join(MYSQL_INFO_SCHEMA_TCOLUMNS)
				+ " from INFORMATION_SCHEMA.COLUMNS where table_schema in ( "
				+ Joiner.on(",").join(realSchemaSet) + " ) and table_name in ( "
				+ Joiner.on(",").join(tableSet) + " )";


		sql2 = "select " + Joiner.on(",").join(MYSQL_INFO_SCHEMA_TSTATISTICS)
				+ " from INFORMATION_SCHEMA.STATISTICS where table_schema in ( "
				+ Joiner.on(",").join(realSchemaSet) + " ) and table_name in ( "
				+ Joiner.on(",").join(tableSet) + " )";

		if(LOGGER.isDebugEnabled()) {
			LOGGER.debug("fetch information_schema.columns data : " + sql1);
			LOGGER.debug("fetch information_schema.statistics data : " + sql2);
		}

		this.handler2 = new CheckTableIdxDefConsistencyHandler(null, schemaName, sql2);
		this.handler1 = new CheckTableColDefConsistencyHandler(null, schemaName, sql1);
		this.handler1.setNextHandler(this.handler2);
		this.handler1.handle();
	}

	public void handleWithManagerConnection() throws Exception {

		
		ManagerConnection c = this.source;
		Map<String, SchemaConfig> schemaMap = MycatServer.getInstance().getConfig().getSchemas();
		// 1. 获取选择的schema
		SchemaConfig selSchemaCfg = schemaMap.get(this.schemaName);
		if(selSchemaCfg == null) {
			c.writeErrMessage(ErrorCode.ER_YES, "can not find schema [ " + this.schemaName + " ] in mycat schema.xml");
			return ;
		}
		// 2. 收集对应的分片表和全局表,待下一步处理
		List<TableConfig> tableCfgList = new ArrayList<TableConfig>();
		Set<String> tableSet = new TreeSet<String>();
		for(String tableName : selSchemaCfg.getTables().keySet()) {
			TableConfig tableCfg = selSchemaCfg.getTables().get(tableName);
			if(tableCfg.getRule() != null || tableCfg.isGlobalTable()) {
				tableCfgList.add(tableCfg);
				tableSet.add("'" + tableName + "'");
			}
		}
		// 如果选择的schema没有分片表或者全局表, 返回提示信息给客户端
		if (tableSet.size() <= 0) {
		    c.writeErrMessage(ErrorCode.ER_YES, "can not find any shard table or global table in schema '" + this.schemaName + "'");
		    return ;
		}
		
		Set<String> realSchemaSet = getRealSchemaSet(tableCfgList);
		sql1 = "select " + Joiner.on(",").join(MYSQL_INFO_SCHEMA_TCOLUMNS)
				+ " from INFORMATION_SCHEMA.COLUMNS where table_schema in ( "
				+ Joiner.on(",").join(realSchemaSet) + " ) and table_name in ( "
				+ Joiner.on(",").join(tableSet) + " )";
		
		
		sql2 = "select " + Joiner.on(",").join(MYSQL_INFO_SCHEMA_TSTATISTICS)
				+ " from INFORMATION_SCHEMA.STATISTICS where table_schema in ( "
				+ Joiner.on(",").join(realSchemaSet) + " ) and table_name in ( "
				+ Joiner.on(",").join(tableSet) + " )";
		
		if(LOGGER.isDebugEnabled()) {
			LOGGER.debug("fetch information_schema.columns data : " + sql1);
			LOGGER.debug("fetch information_schema.statistics data : " + sql2);
		}
		
		this.handler2 = new CheckTableIdxDefConsistencyHandler(c, schemaName, sql2);
		this.handler1 = new CheckTableColDefConsistencyHandler(c, schemaName, sql1);
		this.handler1.setNextHandler(this.handler2);
		this.handler1.handle();
		
	}
	
	
	/**
	 * 获取表真实所在的database,以Set返回(考虑当表所在的database不同时,有多个database)
	 * @param tableCfgList
	 * @return
	 */
	private Set<String> getRealSchemaSet(List<TableConfig> tableCfgList) {
		Set<String> dataNodeSet = new TreeSet<String>();
		for(TableConfig tableCfg : tableCfgList) {
			dataNodeSet.addAll(tableCfg.getDataNodes());
		}
		Map<String, PhysicalDBNode> dataNodes = MycatServer.getInstance().getConfig().getDataNodes();
		Set<String> realSchemaSet = new TreeSet<String>();
		for(String dataNode : dataNodeSet) {
			realSchemaSet.add("'" + dataNodes.get(dataNode).getDatabase() + "'");
		}
		return realSchemaSet;
	}
	
	private String getCombineKey(String combineStr, String... keys) {
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < keys.length; i++) {
			if(i == keys.length - 1) {
				sb.append(keys[i]);
			} else {
				sb.append(keys[i] + combineStr);
			}
		}
		return sb.toString();
	}
	
	private String[] unCombineKey(String combineStr, String combineKey) {
		String[] keys = combineKey.split("\\" + combineStr);
		return keys;
	}
	
	private void getDhDbToDnMap() {
		dhDbToDnMap = new HashMap<String, String>();
		Map<String, PhysicalDBNode> dataNodes = MycatServer.getInstance().getConfig().getDataNodes();
		for(PhysicalDBNode dataNode : dataNodes.values()) {
			String dn = dataNode.getName();
			String db = dataNode.getDatabase();
			String dh = dataNode.getDbPool().getHostName();
			dhDbToDnMap.put(getCombineKey(SPLIT_STR, dh, db), dn);
		}
	}
	
	private String printAddItem(AddItem addItem) {
		StringBuilder sb = new StringBuilder();
		Map<String, PhysicalDBNode> dataNodes = MycatServer.getInstance().getConfig().getDataNodes();
		String dh = dataNodes.get(addItem.dn).getDbPool().getHostName();
		String dn = dhDbToDnMap.get(getCombineKey(SPLIT_STR, dh, addItem.db));
		sb.append(dn + " -> " + addItem.key + " " + addItem.value);
		return sb.toString();
	}
	
	private String printDelItem(DeleteItem delItem) {
		StringBuilder sb = new StringBuilder();
		Map<String, PhysicalDBNode> dataNodes = MycatServer.getInstance().getConfig().getDataNodes();
		String dh = dataNodes.get(delItem.dn).getDbPool().getSource().getConfig().getHostName();
		String dn = dhDbToDnMap.get(getCombineKey(SPLIT_STR, dh, delItem.db));
		sb.append(dn + " -> " + delItem.key + " " + delItem.value);
		return sb.toString();
	}
	
	private String printDiffGroup(DiffItem diffItem) {
		Map<String, PhysicalDBNode> dataNodes = MycatServer.getInstance().getConfig().getDataNodes();
		List<String> strList = new ArrayList<String>();
		for(String key : diffItem.diffGroup.keySet()) {
			Set<String> urlSet = new TreeSet<String>();
			for(String value : diffItem.diffGroup.get(key)) {
				String[] dnAndDb = unCombineKey(SPLIT_STR, value);
				String dn = dnAndDb[0];
				String db = dnAndDb[1];
				String dh = dataNodes.get(dn).getDbPool().getHostName();
				urlSet.add(dhDbToDnMap.get(getCombineKey(SPLIT_STR, dh, db)));
			}
			strList.add("=>[{ " + Joiner.on(",").join(urlSet) + " } -> " + "{ " + key + " }]");
		}
		return Joiner.on(", " + LINE_SEP).join(strList);
	}

	private void response(Map<String, TableCheckResult> resultMap) throws IOException {
		if (isManagerConnection) {
			responseWithManagerConnection(resultMap);
		}else
		{
			responseNotWithManagerConnection(resultMap);
		}
	}


	private void responseNotWithManagerConnection(Map<String, TableCheckResult> resultMap) throws IOException
	{
			getDhDbToDnMap();

			//String hr = "\n";
			StringWriter strWriter = new StringWriter();
			BufferedWriter bufWriter = new BufferedWriter(strWriter);
			boolean consistent = true;
			for(String tableName : resultMap.keySet()) {
				TableCheckResult result = resultMap.get(tableName);
				if(result.isConsistent()) {
					continue;
				}
				consistent = false;


				bufWriter.write("TABLE : " + tableName);
				if(!result.isColDefConsistent()) {
					bufWriter.write("[COLUMN DEFINE] : ");
					if(result.addColItems.size() > 0) {
						bufWriter.write("[ADD] : ");
						int no = 1;
						for(AddItem addItem : result.addColItems) {
							bufWriter.write("(" + no + ") " + printAddItem(addItem));
							bufWriter.newLine();
							no++;
						}
					}
					if(result.delColItems.size() > 0) {
						bufWriter.write("[DELETE] : ");
						int no = 1;
						for(DeleteItem delItem : result.delColItems) {
							bufWriter.write("(" + no + ") " + printDelItem(delItem));
							bufWriter.newLine();
							no++;
						}
					}
					if(result.diffColItems.size() > 0) {
						bufWriter.write("[DIFF] : ");
						int no = 1;
						for(DiffItem diffItem : result.diffColItems) {
							bufWriter.write("(" + no + ") " + diffItem.key + " : " + LINE_SEP + printDiffGroup(diffItem));
							bufWriter.newLine();
							no++;
						}
					}
				}
				if(!result.isIdxDefConsistent()) {
					bufWriter.write("[INDEX DEFINE] : ");
					if(result.addIdxItems.size() > 0) {
						bufWriter.write("[ADD] : ");
						int no = 1;
						for(AddItem addItem : result.addIdxItems) {
							bufWriter.write("(" + no + ") " + printAddItem(addItem));
							bufWriter.newLine();
							no++;
						}
					}
					if(result.delIdxItems.size() > 0) {
						bufWriter.write("[DELETE] : ");
						int no = 1;
						for(DeleteItem delItem : result.delIdxItems) {
							bufWriter.write("(" + no + ") " + printDelItem(delItem));
							no++;
						}
					}
					if(result.diffIdxItems.size() > 0) {
						bufWriter.write("[DIFF] : ");
						int no = 1;
						for(DiffItem diffItem : result.diffIdxItems) {
							bufWriter.write("(" + no + ")" + diffItem.key + " : " + LINE_SEP + printDiffGroup(diffItem));
							no++;
						}
					}
				}
			}

		bufWriter.flush();
		CheckTableStructureConsistencyInfo checkTscInfo = new CheckTableStructureConsistencyInfo();

		if(!consistent){
			checkTscInfo.setSchemaName(schemaName);
			checkTscInfo.setConsistency("NO");
			checkTscInfo.setDesc(strWriter.toString().replaceAll("'","").replaceAll(LINE_SEP,"   -|-   "));
		} else {
			checkTscInfo.setSchemaName(schemaName);
			checkTscInfo.setConsistency("OK");
			checkTscInfo.setDesc("Table Structure Consistency All The Same !");
		}
		checkTscInfo.update();
	}

	private void responseWithManagerConnection(Map<String, TableCheckResult> resultMap) throws IOException {
		
		getDhDbToDnMap();
		
		ManagerConnection c = this.source;
		ByteBuffer buffer = c.allocate();

		// write header
		buffer = header.write(buffer, c, true);

		// write fields
		for (FieldPacket field : fields) {
			buffer = field.write(buffer, c, true);
		}

		// write eof
		buffer = eof.write(buffer, c, true);

		// write rows
		byte packetId = eof.packetId;
		
		String hr = "---------------------------------------------------------------------";
		
		String charset = c.getCharset();
		StringWriter strWriter = new StringWriter();
		BufferedWriter bufWriter = new BufferedWriter(strWriter);
		boolean consistent = true;
		for(String tableName : resultMap.keySet()) {
			TableCheckResult result = resultMap.get(tableName);
			if(result.isConsistent()) {
				continue;
			}
			consistent = false;
			bufWriter.newLine();
			bufWriter.write(hr);
			bufWriter.newLine();
			bufWriter.write("TABLE : " + tableName);
			if(!result.isColDefConsistent()) {
				bufWriter.newLine();
				bufWriter.write("[COLUMN DEFINE] : ");
				bufWriter.newLine();
//				bufWriter.write("[COMM]");
//				bufWriter.newLine();
//				bufWriter.write(result.commColNameSet.toString());
//				bufWriter.newLine();
				if(result.addColItems.size() > 0) {
					bufWriter.write("[ADD] : ");
					bufWriter.newLine();
					int no = 1;
					for(AddItem addItem : result.addColItems) {
						bufWriter.write("(" + no + ") " + printAddItem(addItem));
						bufWriter.newLine();
						no++;
					}
				}
				if(result.delColItems.size() > 0) {
					bufWriter.write("[DELETE] : ");
					bufWriter.newLine();
					int no = 1;
					for(DeleteItem delItem : result.delColItems) {
						bufWriter.write("(" + no + ") " + printDelItem(delItem));
						bufWriter.newLine();
						no++;
					}
				}
				if(result.diffColItems.size() > 0) {
					bufWriter.write("[DIFF] : ");
					bufWriter.newLine();
					int no = 1;
					for(DiffItem diffItem : result.diffColItems) {
						bufWriter.write("(" + no + ") " + diffItem.key + " : " + LINE_SEP + printDiffGroup(diffItem));
						bufWriter.newLine();
						no++;
					}
				}
			}
			if(!result.isIdxDefConsistent()) {
				bufWriter.newLine();
				bufWriter.write("[INDEX DEFINE] : ");
				bufWriter.newLine();
//				bufWriter.write("[COMM]");
//				bufWriter.newLine();
//				bufWriter.write(result.commIdxNameSet.toString());
//				bufWriter.newLine();
				if(result.addIdxItems.size() > 0) {
					bufWriter.write("[ADD] : ");
					bufWriter.newLine();
					int no = 1;
					for(AddItem addItem : result.addIdxItems) {
						bufWriter.write("(" + no + ") " + printAddItem(addItem));
						bufWriter.newLine();
						no++;
					}
				}
				if(result.delIdxItems.size() > 0) {
					bufWriter.write("[DELETE] : ");
					bufWriter.newLine();
					int no = 1;
					for(DeleteItem delItem : result.delIdxItems) {
						bufWriter.write("(" + no + ") " + printDelItem(delItem));
						bufWriter.newLine();
						no++;
					}
				}
				if(result.diffIdxItems.size() > 0) {
					bufWriter.write("[DIFF] : ");
					bufWriter.newLine();
					int no = 1;
					for(DiffItem diffItem : result.diffIdxItems) {
						bufWriter.write("(" + no + ")" + diffItem.key + " : " + LINE_SEP + printDiffGroup(diffItem));
						bufWriter.newLine();
						no++;
					}
				}
			}
		}
		bufWriter.flush();
		
		String response = null;
		if(!consistent) {
			response = LINE_SEP + "CONSISTENCY : NO" + LINE_SEP;
			response = response + strWriter.toString();
		} else {
			response = "CONSISTENCY : OK";
		}
		
		RowDataPacket rowPkg = new RowDataPacket(FIELD_COUNT);
		rowPkg.add(ByteUtil.getBytes(response, charset));
		rowPkg.packetId = ++packetId;
		buffer = rowPkg.write(buffer, c, true);
		
		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.write(buffer, c, true);

		// write buffer
		c.write(buffer);
	}
	
}
