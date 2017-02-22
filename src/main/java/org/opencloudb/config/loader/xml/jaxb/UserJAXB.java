package org.opencloudb.config.loader.xml.jaxb;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.opencloudb.config.model.UserConfig;

import com.google.common.base.Joiner;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(namespace = "http://org.opencloudb/", name = "user")
public class UserJAXB {
	
	@XmlElement(name = "user")
	private List<User> users = new ArrayList<User>();
	
	@XmlAccessorType(XmlAccessType.FIELD) @XmlType(name = "user")
	public static class User {
		
		@XmlAttribute(required = true)
		private String name;
		
		@XmlElement(name = "property")
		private List<Property> properties = new ArrayList<Property>();
		
		public static User transferFrom(UserConfig userConf) {
			User user = new User();
			user.setName(userConf.getName());
			user.getProperties().add(new Property("password", userConf.getPassword()));
			user.getProperties().add(new Property("schemas", Joiner.on(",").join(new TreeSet<String>(userConf.getSchemas()))));
			return user;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public List<Property> getProperties() {
			return properties;
		}

		public void setProperties(List<Property> properties) {
			this.properties = properties;
		}
		
	}

	public List<User> getUsers() {
		return users;
	}

	public void setUsers(List<User> users) {
		this.users = users;
	}
	

}
