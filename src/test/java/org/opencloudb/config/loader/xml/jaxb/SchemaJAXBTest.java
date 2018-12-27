package org.opencloudb.config.loader.xml.jaxb;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.junit.Test;
import org.opencloudb.config.loader.xml.jaxb.SchemaJAXB.Schema;
import org.opencloudb.config.loader.xml.jaxb.SchemaJAXB.Schema.Table;
import org.opencloudb.config.loader.xml.jaxb.SchemaJAXB.Schema.Table.ChildTable;

public class SchemaJAXBTest {
	
	@Test
	public void simpleTest() {
		SchemaJAXB schemaJAXB = new SchemaJAXB();
		Schema schema = new Schema();
		schema.setName("testdb");
		schema.setSqlMaxLimit(10000);
		schema.setCheckSQLschema(false);
		schemaJAXB.getSchemas().add(schema);
		
		Table table = new Table();
		table.setName("customer");
		table.setDataNode("dn1,dn2,dn3");
		table.setRule("mod3");
		
		ChildTable childTable = new ChildTable();
		childTable.setName("orders");
		childTable.setParentKey("id");
		childTable.setJoinKey("customer_id");
		
		table.getChildTable().add(childTable);
		
		schema.getTable().add(table);
		
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(SchemaJAXB.class);
			Marshaller marshaller = jaxbContext.createMarshaller();
			marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
			marshaller.marshal(schemaJAXB, System.out);
		} catch(JAXBException e) {
			e.printStackTrace();
		}
	}

}
