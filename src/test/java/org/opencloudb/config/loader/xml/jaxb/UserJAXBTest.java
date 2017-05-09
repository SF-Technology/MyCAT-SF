package org.opencloudb.config.loader.xml.jaxb;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.junit.Test;
import org.opencloudb.config.loader.xml.jaxb.UserJAXB.User;

public class UserJAXBTest {
	
	@Test
	public void simpleTest() {
		
		UserJAXB userJAXB = new UserJAXB();
		
		User user = new User();
		user.setName("root");
		user.getProperties().add(new Property("password", "mysql"));
		user.getProperties().add(new Property("schemas", "testdb, cjx"));
		
		userJAXB.getUsers().add(user);
		
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(UserJAXB.class);
			Marshaller marshaller = jaxbContext.createMarshaller();
			marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
			marshaller.marshal(userJAXB, System.out);
		} catch(JAXBException e) {
			e.printStackTrace();
		}
		
	}

}
