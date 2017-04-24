package org.opencloudb.config.loader.xml.jaxb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.opencloudb.config.loader.xml.jaxb.SchemaJAXB.Schema.Table.ChildTable;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.TableConfig;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(namespace = "http://org.opencloudb/", name = "schema")
public class SchemaJAXB {
	
	@XmlElement(name = "schema")
	private List<Schema> schemas = new ArrayList<Schema>();
	
	@XmlAccessorType(XmlAccessType.FIELD)
	@XmlType(name = "schema")
	public static class Schema {
		
		@XmlAttribute(required = true)
		private String name;
		@XmlAttribute
		private boolean checkSQLschema;
		@XmlAttribute
		private int sqlMaxLimit;
		@XmlAttribute
		private String dataNode;
		
		private List<Table> table = new ArrayList<Table>();
		
		public static Schema transferFrom(SchemaConfig schemaConf) {
			Schema schema = new Schema();
			schema.setName(schemaConf.getName());
			schema.setCheckSQLschema(schemaConf.isCheckSQLSchema());
			schema.setSqlMaxLimit(schemaConf.getDefaultMaxLimit());
			if(schemaConf.getDataNode() != null) {
				schema.setDataNode(schemaConf.getDataNode());
			}
			
			Map<String, Table> tableIndexMap = new HashMap<String, SchemaJAXB.Schema.Table>();
			Map<String, List<ChildTable>> childTableListIndexMap = new HashMap<String, List<ChildTable>>();
			Set<String> rootTableSet = new HashSet<String>();
			Map<String, ChildTable> childTableMap = new HashMap<String, ChildTable>();
			
			// table的处理
			for(TableConfig tableConf : schemaConf.getTables().values()) {
				if(tableConf.getParentTC() == null) {
					Table table = Table.transferFrom(tableConf);
					tableIndexMap.put(tableConf.getName(), table);
					childTableListIndexMap.put(tableConf.getName(), table.getChildTable());
					rootTableSet.add(tableConf.getName());
				} else {
					ChildTable childTable = ChildTable.transferFrom(tableConf);
					childTableMap.put(tableConf.getName(), childTable);
					childTableListIndexMap.put(tableConf.getName(), childTable.getChildTable());
				}
			}
			
			Set<String> childTableSet = new HashSet<String>(schemaConf.getTables().keySet());
			childTableSet.removeAll(rootTableSet);
			
			// childtable的处理
			for(String childTableName : childTableSet) {
				TableConfig tableConf = schemaConf.getTables().get(childTableName);
				String parentTable = tableConf.getParentTC().getName();
				ChildTable childTable = childTableMap.get(childTableName);
				childTableListIndexMap.get(parentTable).add(childTable);
			}
			
			if(tableIndexMap.size() > 0) {
				for(String tableName : new TreeSet<String>(tableIndexMap.keySet())) {
					schema.getTable().add(tableIndexMap.get(tableName));
				}
			}
			
			return schema;
		}
		
		@XmlAccessorType(XmlAccessType.FIELD)
		@XmlType(name = "table")
		public static class Table {
			
			@XmlAttribute(required = true)
			private String name;
			@XmlAttribute
			private String type;
			@XmlAttribute
			private String primaryKey;
			@XmlAttribute
			private Boolean autoIncrement;
			@XmlAttribute(required = true)
			private String dataNode;
			@XmlAttribute
			private String rule;
			@XmlAttribute
			private Boolean needAddLimit;
			
			private List<ChildTable> childTable = new ArrayList<ChildTable>();
			
			public static Table transferFrom(TableConfig tableConf) {
				Table table = new Table();
				table.setName(tableConf.getName().toLowerCase());
				table.setDataNode(tableConf.getDataNode());
				if(tableConf.isAutoIncrement()) {
					table.setAutoIncrement(new Boolean(true));
				}
				if(tableConf.getPrimaryKey() != null) {
					table.setPrimaryKey(tableConf.getPrimaryKey().toLowerCase());
				}
				if(tableConf.getRule() != null) {
					table.setRule(tableConf.getRule().getName());
				}
				if(tableConf.isGlobalTable()) {
					table.setType("global");
				}
				if(tableConf.isNeedAddLimit()) {
					table.setNeedAddLimit(new Boolean(true));
				}
				return table;
			} 
			
			
			public String getName() {
				return name;
			}

			public void setName(String name) {
				this.name = name;
			}

			public String getDataNode() {
				return dataNode;
			}


			public void setDataNode(String dataNode) {
				this.dataNode = dataNode;
			}

			public String getRule() {
				return rule;
			}

			public void setRule(String rule) {
				this.rule = rule;
			}

			public String getPrimaryKey() {
				return primaryKey;
			}

			public void setPrimaryKey(String primaryKey) {
				this.primaryKey = primaryKey;
			}

			public boolean isAutoIncrement() {
				return autoIncrement;
			}

			public void setAutoIncrement(boolean autoIncrement) {
				this.autoIncrement = autoIncrement;
			}

			public String getType() {
				return type;
			}

			public void setType(String type) {
				this.type = type;
			}
			

			public Boolean getNeedAddLimit() {
				return needAddLimit;
			}


			public void setNeedAddLimit(Boolean needAddLimit) {
				this.needAddLimit = needAddLimit;
			}


			public List<ChildTable> getChildTable() {
				return childTable;
			}

			public void setChildTable(List<ChildTable> childTable) {
				this.childTable = childTable;
			}

			@XmlAccessorType(XmlAccessType.FIELD)
			@XmlType(name = "childtable")
			public static class ChildTable {
				
				@XmlAttribute(required = true)
				private String name;
				@XmlAttribute
                private String primaryKey;
                @XmlAttribute
                private Boolean autoIncrement;
                @XmlAttribute(required = true)
                private String joinKey;
                @XmlAttribute(required = true)
                private String parentKey;
                @XmlAttribute
                private Boolean needAddLimit;
                
                private List<ChildTable> childTable = new ArrayList<ChildTable>();
                
                public static ChildTable transferFrom(TableConfig tableConf) {
                	ChildTable childTable = new ChildTable();
                	childTable.setName(tableConf.getName().toLowerCase());
                	childTable.setJoinKey(tableConf.getJoinKey().toLowerCase());
                	childTable.setParentKey(tableConf.getParentKey().toLowerCase());
                	if(tableConf.isAutoIncrement()) {
                		childTable.setAutoIncrement(new Boolean(true));
                	}
                	if(tableConf.getPrimaryKey() != null) {
                		childTable.setPrimaryKey(tableConf.getPrimaryKey().toLowerCase());
                	}
                	if(tableConf.isNeedAddLimit()) {
                		childTable.setNeedAddLimit(new Boolean(true));
                	}
                	return childTable;
                }

				public String getName() {
					return name;
				}

				public void setName(String name) {
					this.name = name;
				}

				public String getJoinKey() {
					return joinKey;
				}

				public void setJoinKey(String joinKey) {
					this.joinKey = joinKey;
				}

				public String getParentKey() {
					return parentKey;
				}

				public void setParentKey(String parentKey) {
					this.parentKey = parentKey;
				}

				public String getPrimaryKey() {
					return primaryKey;
				}

				public void setPrimaryKey(String primaryKey) {
					this.primaryKey = primaryKey;
				}

				public Boolean getAutoIncrement() {
					return autoIncrement;
				}

				public void setAutoIncrement(Boolean autoIncrement) {
					this.autoIncrement = autoIncrement;
				}
				

				public Boolean getNeedAddLimit() {
					return needAddLimit;
				}

				public void setNeedAddLimit(Boolean needAddLimit) {
					this.needAddLimit = needAddLimit;
				}

				public List<ChildTable> getChildTable() {
					return childTable;
				}

				public void setChildTables(List<ChildTable> childTable) {
					this.childTable = childTable;
				}
                
			}
			
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public boolean isCheckSQLschema() {
			return checkSQLschema;
		}

		public void setCheckSQLschema(boolean checkSQLschema) {
			this.checkSQLschema = checkSQLschema;
		}

		public int getSqlMaxLimit() {
			return sqlMaxLimit;
		}

		public void setSqlMaxLimit(int sqlMaxLimit) {
			this.sqlMaxLimit = sqlMaxLimit;
		}

		public String getDataNode() {
			return dataNode;
		}

		public void setDataNode(String dataNode) {
			this.dataNode = dataNode;
		}

		public List<Table> getTable() {
			return table;
		}

		public void setTables(List<Table> table) {
			this.table = table;
		}
		
	}

	public List<Schema> getSchemas() {
		return schemas;
	}

	public void setSchemas(List<Schema> schemas) {
		this.schemas = schemas;
	}
	
}
