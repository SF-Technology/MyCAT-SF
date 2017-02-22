package org.opencloudb.config.util;

import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;

import org.opencloudb.MycatConfig;
import org.opencloudb.config.loader.xml.jaxb.SchemaJAXB;
import org.opencloudb.config.loader.xml.jaxb.UserJAXB;
import org.opencloudb.config.loader.xml.jaxb.UserJAXB.User;
import org.opencloudb.config.loader.xml.jaxb.SchemaJAXB.Schema;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.UserConfig;

/**
 * 
 * @author CrazyPig
 * @since 2017-02-22
 *
 */
public class JAXBUtil {
	
	/**
	 * 对象转换成xml文件
	 * @param object 待转换成xml的对象
	 * @param fileName 生成的xml文件名
	 * @param dtdName dtd文件名称(不带后缀.dtd)
	 * @throws Exception
	 */
	public static void marshaller(Object object, String fileName, String dtdName) throws Exception {

		JAXBContext jaxbContext = JAXBContext.newInstance(object.getClass());
		Marshaller marshaller = jaxbContext.createMarshaller();
		marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
		marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);

		marshaller.setProperty("com.sun.xml.internal.bind.xmlHeaders",
				String.format("<!DOCTYPE mycat:%1$s SYSTEM \"%1$s.dtd\">", dtdName));

		URL url = JAXBUtil.class.getClassLoader().getResource(fileName);
		if(url == null) {
			throw new Exception("can not found file : " + fileName);
		}
		
		Path path = Paths.get(url.toURI());
		
		try (OutputStream out = Files.newOutputStream(path, StandardOpenOption.CREATE,
				StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
			marshaller.marshal(object, out);
		}
		
	}
	
	public static void flushSchema(MycatConfig mycatConfig) throws Exception {
		
		SchemaJAXB schemaJAXB = new SchemaJAXB();
	
		if(mycatConfig.getSchemas().size() > 0) {
			Set<String> schemaNameSet = new TreeSet<String>(mycatConfig.getSchemas().keySet());
			for(String schemaName : schemaNameSet) {
				SchemaConfig schemaConf = mycatConfig.getSchemas().get(schemaName);
				Schema schema = Schema.transferFrom(schemaConf);
				schemaJAXB.getSchemas().add(schema);
			}
		}
		
		// 刷新到schema.xml
		marshaller(schemaJAXB, "schema.xml", "schema");
		
	}
	
	public static void flushUser(MycatConfig mycatConfig) throws Exception {
		
		UserJAXB userJAXB = new UserJAXB();
		
		if(mycatConfig.getUsers().size() > 0) {
			Set<String> userNameSet = new TreeSet<String>(mycatConfig.getUsers().keySet());
			for(String userName : userNameSet) {
				if(mycatConfig.getSystem().getRootUser().equals(userName)) {
					continue;
				}
				UserConfig userConf = mycatConfig.getUsers().get(userName);
				User user = User.transferFrom(userConf);
				userJAXB.getUsers().add(user);
			}
		}
		
		// 刷新到user.xml
		marshaller(userJAXB, "user.xml", "user");
		
	}
	
}
