package org.opencloudb.config.loader.xml;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.config.model.UserConfig;
import org.opencloudb.config.util.ConfigException;
import org.opencloudb.config.util.ConfigUtil;
import org.opencloudb.util.DecryptUtil;
import org.opencloudb.util.SplitUtil;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * 从server.xml分离user标签到user.xml后, 该类负责解析user.xml
 * @author CrazyPig
 * @since 2017-02-21
 *
 */
public final class XMLUserLoader {
	
	private final Map<String, UserConfig> users;
	private final SystemConfig system;
	
	public XMLUserLoader(SystemConfig system) {
		this.system = system;
		this.users = new HashMap<String, UserConfig>();
		this.load();
	}
	
	private void load() {
		InputStream dtd = null;
        InputStream xml = null;
        try {
        	dtd = XMLServerLoader.class.getResourceAsStream("/user.dtd");
            xml = XMLServerLoader.class.getResourceAsStream("/user.xml");
            Element root = ConfigUtil.getDocument(dtd, xml).getDocumentElement();
        	loadUsers(root);
        } catch(ConfigException e) {
        	throw e;
        } catch(Exception e) {
        	throw new ConfigException(e);
        } finally {
            if (dtd != null) {
                try {
                    dtd.close();
                } catch (IOException e) {
                }
            }
            if (xml != null) {
                try {
                    xml.close();
                } catch (IOException e) {
                }
            }
        }
	}
	
	private void loadUsers(Element root) {
        NodeList list = root.getElementsByTagName("user");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;
                String name = e.getAttribute("name");
//                if(system.getRootUser().equalsIgnoreCase(name)) {
//                	throw new ConfigException("user '" + name + "' has been defined as built-in root user, "
//                			+ "if your need to use this user, "
//                			+ "change the built-in root user defined in server.xml ,"
//                			+ "using system property : <rootUser>${root_username}</rootUser>");
//                }
                UserConfig user = new UserConfig();
                Map<String, Object> props = ConfigUtil.loadElements(e);
                String usingDecrypt = (String)props.get("usingDecrypt");
                String passwordDecrypt = DecryptUtil.mycatDecrypt(usingDecrypt,name,(String)props.get("password"));
                user.setName(name);
                user.setPassword(passwordDecrypt);
                user.setEncryptPassword((String)props.get("password"));
				
				String benchmark = (String) props.get("benchmark");
				if(null != benchmark) {
					user.setBenchmark( Integer.parseInt(benchmark) );
				}
				
				String benchmarkSmsTel = (String) props.get("benchmarkSmsTel");
				if(null != benchmarkSmsTel) {
					user.setBenchmarkSmsTel( benchmarkSmsTel );
				}
				
				String readOnly = (String) props.get("readOnly");
				if (null != readOnly) {
					user.setReadOnly(Boolean.valueOf(readOnly));
				}
				
				String schemas = (String) props.get("schemas");
                if (schemas != null) {
                    String[] strArray = SplitUtil.split(schemas, ',', true);
                    user.setSchemas(new HashSet<String>(Arrays.asList(strArray)));
                } else {
                	user.setSchemas(new HashSet<String>());
                }
                if (users.containsKey(name)) {
                    throw new ConfigException("user " + name + " duplicated!");
                }
                users.put(name, user);
            }
        }
    }
	
	public Map<String, UserConfig> getUsers() {
		return this.users;
	}

}
