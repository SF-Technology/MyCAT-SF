package org.opencloudb.config.util;

import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.backend.PhysicalDBPool;
import org.opencloudb.config.loader.xml.jaxb.DatabaseJAXB;
import org.opencloudb.config.loader.xml.jaxb.RuleJAXB;
import org.opencloudb.config.loader.xml.jaxb.SchemaJAXB;
import org.opencloudb.config.loader.xml.jaxb.UserJAXB;
import org.opencloudb.config.loader.xml.jaxb.UserJAXB.User;
import org.opencloudb.config.loader.xml.jaxb.SchemaJAXB.Schema;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.UserConfig;
import org.opencloudb.config.model.rule.TableRuleConfig;
import org.opencloudb.route.function.AbstractPartitionAlgorithm;

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
	 * @return true:成功 , false:失败
	 */
	public static boolean marshaller(Object object, String fileName, String dtdName) throws Exception {

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
		
		Path target = Paths.get(url.toURI());
		
		OutputStream out = null;
		
		try {
			// 创建临时文件, 将内容先刷到临时文件
			Path tmpFilePath = Files.createTempFile(target.getParent(), fileName, ".tmp");
			out = Files.newOutputStream(tmpFilePath, StandardOpenOption.CREATE,
					StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
			marshaller.marshal(object, out);
			// 原子rename操作, 将临时文件覆盖到目标文件
			Files.move(tmpFilePath, target, StandardCopyOption.ATOMIC_MOVE);
			
		} catch(Exception e) {
			throw e;
		} finally {
			if(out != null) {
				out.close();
			}
		}
		
		return true;
	}
	
	public static boolean flushSchema(SchemaJAXB schemaJAXB) throws Exception {
		
		// 刷新到schema.xml
		return marshaller(schemaJAXB, "schema.xml", "schema");
		
	}
	
	public static boolean flushUser(UserJAXB userJAXB) throws Exception {
		
		// 刷新到user.xml
		return marshaller(userJAXB, "user.xml", "user");
		
	}
	
	/**
	 * 将修改的配置信息刷到rule.xml中
	 * @param ruleJAXB
	 * @return
	 * @throws Exception
	 */
	public static boolean flushRule(RuleJAXB ruleJAXB) throws Exception {
		return marshaller(ruleJAXB, "rule.xml", "rule");
	}
	
	public static boolean flushDatabase(DatabaseJAXB databaseJAXB) throws Exception {
		return marshaller(databaseJAXB, "database.xml", "database");
	}
	
	public static SchemaJAXB toSchemaJAXB(Map<String, SchemaConfig> schemas) {
		
		SchemaJAXB schemaJAXB = new SchemaJAXB();
		Set<String> schemaNameSet = new TreeSet<String>(schemas.keySet());
		for(String schemaName : schemaNameSet) {
			SchemaConfig schemaConf = schemas.get(schemaName);
			Schema schema = Schema.transferFrom(schemaConf);
			schemaJAXB.getSchemas().add(schema);
		}
		
		return schemaJAXB;
	}
	
	public static UserJAXB toUserJAXB(Map<String, UserConfig> users, boolean excludeRoot) {
		
		UserJAXB userJAXB = new UserJAXB();
		Set<String> userNameSet = new TreeSet<String>(users.keySet());
		for(String userName : userNameSet) {
			if(excludeRoot && MycatServer.getInstance().getConfig().getSystem()
					.getRootUser().equals(userName)) {
				continue;
			}
			UserConfig userConf = users.get(userName);
			User user = User.transferFrom(userConf);
			userJAXB.getUsers().add(user);
		}
		
		return userJAXB;
	}
	
	/**
	 * 将内存中的配置信息转化为可以刷入到rule.xml文件中的bean对象
	 * @param currentRules
	 * @param currentFunctions
	 * @return
	 */
	public static RuleJAXB toRuleJAXB(Map<String, TableRuleConfig> currentRules, 
			Map<String, AbstractPartitionAlgorithm> currentFunctions) {
		return new RuleJAXB(currentRules, currentFunctions);
	}
	
	/**
	 * 将内存中的配置信息转化为可以刷入到database.xml文件中的bean对象
	 * @param dataNodes
	 * @param dataHosts
	 * @return
	 */
	public static DatabaseJAXB toDatabaseJAXB (Map<String, PhysicalDBNode> dataNodes, Map<String, PhysicalDBPool> dataHosts) {
		return new DatabaseJAXB(dataNodes, dataHosts);
	}
}
