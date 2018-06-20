package org.opencloudb.config.loader.xml.jaxb;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.junit.Test;
import org.opencloudb.config.loader.xml.jaxb.DatabaseJAXB.DataHost;
import org.opencloudb.config.loader.xml.jaxb.DatabaseJAXB.DataNode;
import org.opencloudb.config.loader.xml.jaxb.DatabaseJAXB.DataHost.ReadHost;
import org.opencloudb.config.loader.xml.jaxb.DatabaseJAXB.DataHost.WriteHost;

public class DatabaseJAXBTest {
	
	@Test
	public void simpleTest() {

		DatabaseJAXB databaseJAXB = new DatabaseJAXB();
		
		DataNode dataNode = new DataNode();
		dataNode.setName("dn1");
		dataNode.setDataHost("localhost");
		dataNode.setDatabase("db1");
		
		databaseJAXB.getDataNodes().add(dataNode);
		
		DataHost dataHost = new DataHost();
		dataHost.setName("localhost");
		dataHost.setBalance(0);
		dataHost.setDbDriver("native");
		dataHost.setDbType("mysql");
		dataHost.setHeartbeat("select user()");
		dataHost.setMaxCon(1000);
		dataHost.setMinCon(5);
		dataHost.setSwitchType(-1);
		
		WriteHost writeHost = new WriteHost();
		writeHost.setHost("w1");
		writeHost.setUrl("localhost:3306");
		writeHost.setUser("root");
		writeHost.setPassword("mysql");
		
		ReadHost readHost = new ReadHost();
		readHost.setHost("r1");
		readHost.setUrl("localhost:3307");
		readHost.setUser("root");
		readHost.setPassword("mysql");
		
		writeHost.getReadHosts().add(readHost);
		
		dataHost.getWriteHosts().add(writeHost);
		
		databaseJAXB.getDataHosts().add(dataHost);
		
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(DatabaseJAXB.class);
			Marshaller marshaller = jaxbContext.createMarshaller();
			marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
			marshaller.marshal(databaseJAXB, System.out);
		} catch(JAXBException e) {
			e.printStackTrace();
		}
		
	}

}
