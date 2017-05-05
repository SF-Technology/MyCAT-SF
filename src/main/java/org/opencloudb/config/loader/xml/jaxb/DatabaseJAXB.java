package org.opencloudb.config.loader.xml.jaxb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.backend.PhysicalDBPool;
import org.opencloudb.config.loader.xml.jaxb.DatabaseJAXB.DataHost.ReadHost;
import org.opencloudb.config.loader.xml.jaxb.DatabaseJAXB.DataHost.WriteHost;
import org.opencloudb.config.model.DBHostConfig;
import org.opencloudb.config.model.DataHostConfig;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(namespace = "http://org.opencloudb/", name = "database")
public class DatabaseJAXB {
	
	@XmlElement(name = "dataNode")
	private List<DataNode> dataNodes = new ArrayList<DataNode>();
	@XmlElement(name = "dataHost")
	private List<DataHost> dataHosts = new ArrayList<DataHost>();
	
	public DatabaseJAXB() {
	}
	
	public DatabaseJAXB(Map<String, PhysicalDBNode> dataNodes, Map<String, PhysicalDBPool> dataHosts) {
		for (PhysicalDBNode physicalDBNode : dataNodes.values()) {
			DataNode dataNode = new DataNode();
			this.dataNodes.add(dataNode);
			
			dataNode.setName(physicalDBNode.getName());
			dataNode.setDataHost(physicalDBNode.getDbPool().getHostName());
			dataNode.setDatabase(physicalDBNode.getDatabase());
		}
		
		for (PhysicalDBPool pool : dataHosts.values()) {
			DataHost dataHost = new DataHost();
			this.dataHosts.add(dataHost);

			DataHostConfig hostConfig = pool.getDataHostConfig();
			dataHost.setName(hostConfig.getName());
			dataHost.setBalance(hostConfig.getBalance());
			dataHost.setMaxCon(hostConfig.getMaxCon());
			dataHost.setMinCon(hostConfig.getMinCon());
			dataHost.setWriteType(hostConfig.getWriteType());
			dataHost.setSwitchType(hostConfig.getSwitchType());
			dataHost.setSlaveThreshold(hostConfig.getSlaveThreshold());
			dataHost.setDbType(hostConfig.getDbType());
			dataHost.setDbDriver(hostConfig.getDbDriver());
	        dataHost.setHeartbeat(hostConfig.getHearbeatSQL());
	        
	        /*
	         * 读取所有的writeHost
	         */
	        List<WriteHost> writeHosts = new ArrayList<WriteHost>();
	        dataHost.setWriteHosts(writeHosts);
	        DBHostConfig[] writeHostArray = hostConfig.getWriteHosts();
	        for (int i = 0; i < writeHostArray.length; i ++) {
	        	DBHostConfig writeHostConfig = writeHostArray[i];
	        	
	        	WriteHost writeHost = new WriteHost();
	        	
	        	writeHost.setHost(writeHostConfig.getHostName());
	        	writeHost.setUrl(writeHostConfig.getUrl());
	        	writeHost.setUser(writeHostConfig.getUser());
	        	writeHost.setPassword(writeHostConfig.getPassword());
	        	
	        	/*
	        	 * 获得writeHost下的所有readHost
	        	 */
	        	List<ReadHost> readHosts = new ArrayList<ReadHost>();
	        	writeHost.setReadHosts(readHosts);
	        	DBHostConfig[] readHostArray = hostConfig.getReadHosts().get(i);
	        	if (readHostArray != null) {
	        		for (DBHostConfig readHostConfig : readHostArray) {
	        			ReadHost readHost = new ReadHost();
	        			readHosts.add(readHost);
	        			
	        			readHost.setHost(readHostConfig.getHostName());
	        			readHost.setUrl(readHostConfig.getUrl());
	        			readHost.setUser(readHostConfig.getUser());
	        			readHost.setPassword(readHostConfig.getPassword());
	        		}
	        	}
	        	
	        	writeHosts.add(writeHost);
	        }
		}
 	}
	
	
	
	@XmlAccessorType(XmlAccessType.FIELD) @XmlType(name = "dataNode")
	public static class DataNode {
		
		@XmlAttribute(required = true)
		private String name;
		@XmlAttribute(required = true)
		private String dataHost;
		@XmlAttribute(required = true)
		private String database;
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getDataHost() {
			return dataHost;
		}
		public void setDataHost(String dataHost) {
			this.dataHost = dataHost;
		}
		public String getDatabase() {
			return database;
		}
		public void setDatabase(String database) {
			this.database = database;
		}
		
	}
	
	@XmlAccessorType(XmlAccessType.FIELD) @XmlType(name = "dataHost")
	public static class DataHost {
		
		@XmlAttribute(required = true)
        private String name;
		@XmlAttribute(required = true)
		private Integer balance;
        @XmlAttribute(required = true)
        private Integer maxCon;
        @XmlAttribute(required = true)
        private Integer minCon;
        @XmlAttribute
        private Integer writeType;
        @XmlAttribute
        private Integer switchType;
        @XmlAttribute
        private Integer slaveThreshold;
        @XmlAttribute(required = true)
        private String dbType;
        @XmlAttribute(required = true)
        private String dbDriver;
        
        @XmlElement(name = "heartbeat")
        private String heartbeat;
        
        @XmlElement(name = "writeHost")
        private List<WriteHost> writeHosts = new ArrayList<WriteHost>();
        
        @XmlAccessorType(XmlAccessType.FIELD) @XmlType(name = "writeHost")
        public static class WriteHost {
        	
        	@XmlAttribute(required = true)
        	private String host;
            @XmlAttribute(required = true)
            private String url;
            @XmlAttribute(required = true)
            private String user;
            @XmlAttribute(required = true)
            private String password;
            
            public String getHost() {
				return host;
			}

			public void setHost(String host) {
				this.host = host;
			}

			public String getUrl() {
				return url;
			}

			public void setUrl(String url) {
				this.url = url;
			}

			public String getPassword() {
				return password;
			}

			public void setPassword(String password) {
				this.password = password;
			}

			public String getUser() {
				return user;
			}

			public void setUser(String user) {
				this.user = user;
			}
            
            @XmlElement(name = "readHost")
            private List<ReadHost> readHosts = new ArrayList<ReadHost>();


			public List<ReadHost> getReadHosts() {
				return readHosts;
			}

			public void setReadHosts(List<ReadHost> readHosts) {
				this.readHosts = readHosts;
			}
            
        	
        }
        
        @XmlAccessorType(XmlAccessType.FIELD) @XmlType(name = "readHost")
        public static class ReadHost {
        	
        	@XmlAttribute(required = true)
        	private String host;
            @XmlAttribute(required = true)
            private String url;
            @XmlAttribute(required = true)
            private String user;
            @XmlAttribute(required = true)
            private String password;
            
            public String getHost() {
				return host;
			}

			public void setHost(String host) {
				this.host = host;
			}

			public String getUrl() {
				return url;
			}

			public void setUrl(String url) {
				this.url = url;
			}

			public String getPassword() {
				return password;
			}

			public void setPassword(String password) {
				this.password = password;
			}

			public String getUser() {
				return user;
			}

			public void setUser(String user) {
				this.user = user;
			}
        }

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Integer getBalance() {
			return balance;
		}

		public void setBalance(Integer balance) {
			this.balance = balance;
		}

		public Integer getMaxCon() {
			return maxCon;
		}

		public void setMaxCon(Integer maxCon) {
			this.maxCon = maxCon;
		}

		public Integer getMinCon() {
			return minCon;
		}

		public void setMinCon(Integer minCon) {
			this.minCon = minCon;
		}

		public Integer getWriteType() {
			return writeType;
		}

		public void setWriteType(Integer writeType) {
			this.writeType = writeType;
		}

		public Integer getSwitchType() {
			return switchType;
		}

		public void setSwitchType(Integer switchType) {
			this.switchType = switchType;
		}

		public Integer getSlaveThreshold() {
			return slaveThreshold;
		}

		public void setSlaveThreshold(Integer slaveThreshold) {
			this.slaveThreshold = slaveThreshold;
		}

		public String getDbType() {
			return dbType;
		}

		public void setDbType(String dbType) {
			this.dbType = dbType;
		}

		public String getDbDriver() {
			return dbDriver;
		}

		public void setDbDriver(String dbDriver) {
			this.dbDriver = dbDriver;
		}

		public String getHeartbeat() {
			return heartbeat;
		}

		public void setHeartbeat(String heartbeat) {
			this.heartbeat = heartbeat;
		}

		public List<WriteHost> getWriteHosts() {
			return writeHosts;
		}

		public void setWriteHosts(List<WriteHost> writeHosts) {
			this.writeHosts = writeHosts;
		}
        
	}

	public List<DataNode> getDataNodes() {
		return dataNodes;
	}

	public void setDataNodes(List<DataNode> dataNodes) {
		this.dataNodes = dataNodes;
	}

	public List<DataHost> getDataHosts() {
		return dataHosts;
	}

	public void setDataHosts(List<DataHost> dataHosts) {
		this.dataHosts = dataHosts;
	}
	
}
