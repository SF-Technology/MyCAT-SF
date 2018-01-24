package org.opencloudb.manager.handler;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.config.model.DataHostConfig;
import org.opencloudb.config.model.DataNodeConfig;
import org.opencloudb.config.model.ProcedureConfig;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.config.model.UserConfig;
import org.opencloudb.config.model.rule.RuleConfig;
import org.opencloudb.config.model.rule.TableRuleConfig;
import org.opencloudb.manager.parser.druid.MycatOutputVisitor;
import org.opencloudb.manager.parser.druid.statement.MycatCreateChildTableStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateDataHostStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateDataNodeStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateFunctionStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateMapFileStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateProcedureStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateRuleStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateSchemaStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateTableStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateUserStatement;
import org.opencloudb.route.function.AbstractPartitionAlgorithm;

import com.google.common.base.Strings;

/**
 * 
 * @author 01169238
 * @since 1.5.3
 *
 */
public class MycatConfigDumper {
    
    private static final String LINE_SEP = System.getProperty("line.separator");
    
    public static void dumpAll(Appendable out) throws IOException {
        MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
        Map<String, SchemaConfig> schemaConfMap = mycatConfig.getSchemas();
        Map<String, UserConfig> userConfMap = getAllUsers();
        Map<String, DataNodeConfig> dnConfMap = getAllDataNodes();
        Map<String, DataHostConfig> dhConfMap = getAllDataHosts();
        Map<String, Map<String, TableConfig>> tableConfMap = getAllTables();
        Map<String, RuleConfig> ruleConfMap = getAllRules();
        Map<String, AbstractPartitionAlgorithm> functionMap = mycatConfig.getFunctions();
        apply(out, schemaConfMap, tableConfMap, ruleConfMap, functionMap, dnConfMap, dhConfMap, userConfMap, true);
        applyProc(out, getAllProcedures());
    }
    
    public static void dump(Appendable out, Map<String, List<String>> tables, boolean dumpSchema) throws IOException {
        
        List<String> schemaNames = new ArrayList<String>(new TreeSet<String>(tables.keySet()));
        // 搜集所有需要dump出来的schema
        Map<String, SchemaConfig> schemaConfMap = getSchemas(schemaNames);
        // 搜集所有需要dump出来的tableConfig
        Map<String, Map<String, TableConfig>> tableConfMap = getTables(tables);
        // 搜集所有需要dump出来的user
        Map<String, UserConfig> userConfMap = getUsers(schemaNames);
        // 搜集所有需要dump出来的dataNode
        Map<String, DataNodeConfig> dnConfMap = getDataNodes(tableConfMap.values());
        // 搜集所有需要dump出来的dataHost
        Map<String, DataHostConfig> dhConfMap = getDataHosts(dnConfMap.keySet());
        // 搜集所有需要dump出来的rule和function, 以及mapfile
        Map<String, RuleConfig> ruleConfMap = getRules(tableConfMap);
        Map<String, AbstractPartitionAlgorithm> functionMap = getFunctions(ruleConfMap);
        
        apply(out, schemaConfMap, tableConfMap, ruleConfMap, functionMap, dnConfMap, dhConfMap, userConfMap, dumpSchema);
        // 如果是dump schema, 需要dump procedure
        if (dumpSchema) {
            applyProc(out, getProcedures(schemaNames));
        }
    }
    
    private static void apply(Appendable out, Map<String, SchemaConfig> schemaConfMap, 
            Map<String, Map<String, TableConfig>> tableConfMap, 
            Map<String, RuleConfig> ruleConfMap,
            Map<String, AbstractPartitionAlgorithm> functionMap,
            Map<String, DataNodeConfig> dnConfMap,
            Map<String, DataHostConfig> dhConfMap,
            Map<String, UserConfig> userConfMap, boolean dumpSchema) throws IOException {
        MycatOutputVisitor outVisitor = new MycatOutputVisitor(out);
        if (dumpSchema) {
            out.append("/* dump schema */" + LINE_SEP);
            for (String schemaName : schemaConfMap.keySet()) {
                MycatCreateSchemaStatement stmt = MycatCreateSchemaStatement.from(schemaConfMap.get(schemaName));
                stmt.accept(outVisitor);
            }
        }
        
        // dump user
        out.append(LINE_SEP);
        out.append("/* dump user */" + LINE_SEP);
        for (UserConfig userConf : userConfMap.values()) {
            MycatCreateUserStatement stmt = MycatCreateUserStatement.from(userConf);
            stmt.accept(outVisitor);
        }
        
        // dump datahost
        out.append(LINE_SEP);
        out.append("/* dump datahost */" + LINE_SEP);
        for (DataHostConfig dhConf : dhConfMap.values()) {
            MycatCreateDataHostStatement stmt = MycatCreateDataHostStatement.from(dhConf);
            stmt.accept(outVisitor);
        }
        
        // dump datanode
        out.append(LINE_SEP);
        out.append("/* dump datanode */" + LINE_SEP);
        for (DataNodeConfig dnConf : dnConfMap.values()) {
            MycatCreateDataNodeStatement stmt = MycatCreateDataNodeStatement.from(dnConf);
            stmt.accept(outVisitor);
        }
        
        Set<String> mapFileNames = getMapFileNames(functionMap);
        // dump mapfile if exist
        if (mapFileNames.size() > 0) {
            try {
                Path basePath = Paths.get(SystemConfig.class.getClassLoader().getResource("").toURI());
                if (basePath != null) {
                    out.append(LINE_SEP);
                    out.append("/* dump mapfile */" + LINE_SEP);
                    for (String mapFileName : mapFileNames) {
                        Path mapFilePath = Paths.get(basePath.toString(), SystemConfig.getMapFileFolder(), mapFileName);
                        // 校验文件存在且不是目录
                        if (!Files.exists(mapFilePath) || Files.isDirectory(mapFilePath)) {
                            continue;
                        }
                        File mapFile = mapFilePath.toFile();
                        MycatCreateMapFileStatement stmt = MycatCreateMapFileStatement.from(mapFile);
                        stmt.accept0(outVisitor);
                    }
                }
            } catch (URISyntaxException e) {
                // ignore
            }
        }
        
        // dump function
        out.append(LINE_SEP);
        out.append("/* dump function */" + LINE_SEP);
        for (String functionName : functionMap.keySet()) {
            AbstractPartitionAlgorithm function = functionMap.get(functionName);
            MycatCreateFunctionStatement stmt = MycatCreateFunctionStatement.from(functionName, function);
            stmt.accept(outVisitor);
        }
        
        // dump rule
        out.append(LINE_SEP);
        out.append("/* dump rule */" + LINE_SEP);
        for (RuleConfig ruleConf : ruleConfMap.values()) {
            MycatCreateRuleStatement stmt = MycatCreateRuleStatement.from(ruleConf);
            stmt.accept(outVisitor);
        }
        
        // dump table
        out.append(LINE_SEP);
        out.append("/* dump table */" + LINE_SEP);
        for (String schemaName : tableConfMap.keySet()) {
            Map<String, TableConfig> map = tableConfMap.get(schemaName);
            for (TableConfig tableConf : map.values()) {
                if (tableConf.getParentTC() != null) {
                    MycatCreateChildTableStatement stmt = MycatCreateChildTableStatement.from(schemaName, tableConf);
                    stmt.accept(outVisitor);
                } else {
                    MycatCreateTableStatement stmt = MycatCreateTableStatement.from(schemaName, tableConf);
                    stmt.accept(outVisitor);
                }
            }
        }
        
    }
    
    private static void applyProc(Appendable out, Map<String, Map<String, ProcedureConfig>> procMap) throws IOException {
        MycatOutputVisitor outVisitor = new MycatOutputVisitor(out);
        // dump procedure
        out.append(LINE_SEP);
        out.append("/* dump procedure */" + LINE_SEP);
        for (String schemaName : procMap.keySet()) {
            for (ProcedureConfig procConf : procMap.get(schemaName).values()) {
                MycatCreateProcedureStatement stmt = MycatCreateProcedureStatement.from(procConf, schemaName);
                stmt.accept(outVisitor);
            }
        }
    }
    
    private static Map<String, SchemaConfig> getSchemas(Collection<String> schemaNames) {
        Map<String, SchemaConfig> schemaConfMap = new TreeMap<String, SchemaConfig>();
        MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
        for (String schemaName : schemaNames) {
            schemaConfMap.put(schemaName, mycatConfig.getSchemas().get(schemaName));
        }
        return schemaConfMap;
    }
    
    /**
     * 获取与schema关联的所有user配置信息
     * @param schemaNames
     * @return
     */
    private static Map<String, UserConfig> getUsers(Collection<String> schemaNames) {
        MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
        Map<String, UserConfig> userConfMap = new TreeMap<String, UserConfig>();
        List<String> userNames = new ArrayList<String>();
        for (String userName : mycatConfig.getUsers().keySet()) {
            UserConfig userConf = mycatConfig.getUsers().get(userName);
            for (String schemaName : schemaNames) {
                // 不dump出默认的root用户
                if (userConf.getSchemas().contains(schemaName) && !userName.equals(mycatConfig.getSystem().getRootUser())) {
                    userNames.add(userName);
                }
            }
        }
        
        for (String userName : userNames) {
            userConfMap.put(userName, mycatConfig.getUsers().get(userName));
        }
        return userConfMap;
    }
    
    private static Map<String, UserConfig> getAllUsers() {
        MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
        Map<String, UserConfig> userConfMap = new TreeMap<String, UserConfig>(mycatConfig.getUsers());
        // 排查默认的root用户
        userConfMap.remove(mycatConfig.getSystem().getRootUser());
        return userConfMap;
    }
    
    /**
     * 获取与schema关联的所有datanode配置信息
     * @param schemaNames
     * @return
     */
//    private static Map<String, DataNodeConfig> getDataNodes(Collection<String> schemaNames) {
//        MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
//        Set<String> dataNodes = new TreeSet<String>();
//        Map<String, DataNodeConfig> dnConfMap = new TreeMap<String, DataNodeConfig>();
//        for (String schemaName : schemaNames) {
//            SchemaConfig schemaConfig = mycatConfig.getSchemas().get(schemaName);
//            dataNodes.addAll(schemaConfig.getAllDataNodes());
//        }
//        for (String dataNode : dataNodes) {
//            PhysicalDBNode realDn = mycatConfig.getDataNodes().get(dataNode);
//            DataNodeConfig dnConf = new DataNodeConfig(realDn.getName(), realDn.getDatabase(), realDn.getDbPool().getHostName());
//            dnConfMap.put(dataNode, dnConf);
//        }
//        return dnConfMap;
//    }
    
    /**
     * 获取与table关联的所有datanode配置信息
     * @param tableConfMaps
     * @return
     */
    private static Map<String, DataNodeConfig> getDataNodes(Collection<Map<String, TableConfig>> tableConfMaps) {
        MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
        Set<String> dataNodes = new TreeSet<String>();
        Map<String, DataNodeConfig> dnConfMap = new TreeMap<String, DataNodeConfig>();
        for (Map<String, TableConfig> tableConfMap : tableConfMaps) {
            for (TableConfig tableConf : tableConfMap.values()) {
                dataNodes.addAll(tableConf.getDataNodes());
            }
        }
        for (String dataNode : dataNodes) {
          PhysicalDBNode realDn = mycatConfig.getDataNodes().get(dataNode);
          DataNodeConfig dnConf = new DataNodeConfig(realDn.getName(), realDn.getDatabase(), realDn.getDbPool().getHostName());
          dnConfMap.put(dataNode, dnConf);
        }
        return dnConfMap;
    }
    
    private static Map<String, DataNodeConfig> getAllDataNodes() {
        Map<String, DataNodeConfig> dnConfMap = new TreeMap<String, DataNodeConfig>();
        MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
        for (String dnName : mycatConfig.getDataNodes().keySet()) {
            PhysicalDBNode realDn = mycatConfig.getDataNodes().get(dnName);
            DataNodeConfig dnConf = new DataNodeConfig(realDn.getName(), realDn.getDatabase(), realDn.getDbPool().getHostName());
            dnConfMap.put(dnName, dnConf);
        }
        return dnConfMap;
    }
    
    /**
     * 获取与datanode关联的所有datahost配置信息
     * @param dataNodes
     * @return
     */
    private static Map<String, DataHostConfig> getDataHosts(Collection<String> dataNodes) {
        MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
        Map<String, DataHostConfig> dhConfMap = new TreeMap<String, DataHostConfig>();
        for (String dataNode : dataNodes) {
            PhysicalDBNode realDn = mycatConfig.getDataNodes().get(dataNode);
            DataHostConfig dhConf = realDn.getDbPool().getDataHostConfig();
            dhConfMap.put(dhConf.getName(), dhConf);
        }
        return dhConfMap;
    }
    
    private static Map<String, DataHostConfig> getAllDataHosts() {
        MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
        Map<String, DataHostConfig> dhConfMap = new TreeMap<String, DataHostConfig>();
        for (String dhName : mycatConfig.getDataHosts().keySet()) {
            dhConfMap.put(dhName, mycatConfig.getDataHosts().get(dhName).getDataHostConfig());
        }
        return dhConfMap;
    }
    
    /**
     * 获取所有需要dump的table配置信息
     * @param tables
     * @return
     */
    private static Map<String, Map<String, TableConfig>> getTables(Map<String, List<String>> tables) {
        Map<String, Map<String, TableConfig>> result = new TreeMap<String, Map<String, TableConfig>>();
        MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
        for (String schemaName : tables.keySet()) {
            SchemaConfig schemaConf = mycatConfig.getSchemas().get(schemaName);
            Map<String, TableConfig> tableConfMap = new TreeMap<String, TableConfig>();
            List<String> tableNames = tables.get(schemaName);
            for (String tableName : tableNames) {
                tableConfMap.put(tableName, schemaConf.getTables().get(tableName));
            }
            result.put(schemaName, tableConfMap);
        }
        return result;
    }
    
    private static Map<String, Map<String, TableConfig>> getAllTables() {
        Map<String, Map<String, TableConfig>> result = new TreeMap<String, Map<String, TableConfig>>();
        MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
        for (String schemaName : mycatConfig.getSchemas().keySet()) {
            SchemaConfig schemaConf = mycatConfig.getSchemas().get(schemaName);
            Map<String, TableConfig> tableConfMap = new TreeMap<String, TableConfig>(schemaConf.getTables());
            result.put(schemaName, tableConfMap);
        }
        return result;
    }
    
    /**
     * 获取与table(筛选出分片表)关联的rule配置信息
     * @param tableConfMap
     * @return
     */
    private static Map<String, RuleConfig> getRules(Map<String, Map<String, TableConfig>> tableConfMap) {
        Map<String, RuleConfig> ruleConfMap = new TreeMap<String, RuleConfig>();
        Set<String> ruleNames = new TreeSet<String>();
        MycatConfig mycatConfig  = MycatServer.getInstance().getConfig();
        for (Map<String, TableConfig> map : tableConfMap.values()) {
            for (TableConfig tableConf : map.values()) {
                // 找出分片表
                if (tableConf.getRule() != null) {
                    ruleNames.add(tableConf.getRule().getName());
                }
            } 
        }
        for (String ruleName : ruleNames) {
            ruleConfMap.put(ruleName, mycatConfig.getTableRules().get(ruleName).getRule());
        }
        return ruleConfMap;
    }
    
    private static Map<String, RuleConfig> getAllRules() {
        Map<String, RuleConfig> ruleConfMap = new TreeMap<String, RuleConfig>();
        MycatConfig mycatConfig  = MycatServer.getInstance().getConfig();
        for (TableRuleConfig tbRuleConf : mycatConfig.getTableRules().values()) {
            ruleConfMap.put(tbRuleConf.getName(), tbRuleConf.getRule());
        }
        return ruleConfMap;
    }
    
    /**
     * 获取与rule关联的function配置信息
     * @param ruleConfMap
     * @return
     */
    private static Map<String, AbstractPartitionAlgorithm> getFunctions(Map<String, RuleConfig> ruleConfMap) {
        Map<String, AbstractPartitionAlgorithm> functionMap = new TreeMap<String, AbstractPartitionAlgorithm>();
        Set<String> functionNames = new TreeSet<String>();
        MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
        for (RuleConfig ruleConf : ruleConfMap.values()) {
            functionNames.add(ruleConf.getFunctionName());
        }
        for (String functionName : functionNames) {
            functionMap.put(functionName, mycatConfig.getFunctions().get(functionName));
        }
        return functionMap;
    }
    
    private static Set<String> getMapFileNames(Map<String, AbstractPartitionAlgorithm> functionMap) {
        Set<String> mapFileNames = new TreeSet<String>();
        for (AbstractPartitionAlgorithm function : functionMap.values()) {
            String mapFile = getMapFileFromFunction(function);
            if (!Strings.isNullOrEmpty(mapFile)) {
                mapFileNames.add(mapFile);
            }
        }
        return mapFileNames;
    }
    
    private static String getMapFileFromFunction(AbstractPartitionAlgorithm function) {
        Field[] fields = function.getClass().getDeclaredFields();
        for (Field field : fields) {
            if ("mapFile".equals(field.getName())) {
                field.setAccessible(true);
                try {
                    Object value = field.get(function);
                    if (value instanceof String) {
                        return value == null ? null : value.toString();
                    }
                } catch (IllegalArgumentException e) {
                    // e.printStackTrace();
                } catch (IllegalAccessException e) {
                    // e.printStackTrace();
                }
            }
        }
        return null;
    }
    
    /**
     * 获取所有与schema关联的procedure
     * @param schemas
     * @return
     */
    private static Map<String, Map<String, ProcedureConfig>> getProcedures(Collection<String> schemas) {
        Map<String, Map<String, ProcedureConfig>> map = new TreeMap<String, Map<String, ProcedureConfig>>();
        MycatConfig mycatConf = MycatServer.getInstance().getConfig();
        for (String schema : schemas) {
            SchemaConfig schemaConf = mycatConf.getSchemas().get(schema);
            if (schemaConf.getProcedures().size() > 0) {
                map.put(schema, new TreeMap<String, ProcedureConfig>(schemaConf.getProcedures()));
            }
        }
        return map;
    }
    
    private static Map<String, Map<String, ProcedureConfig>> getAllProcedures() {
        Map<String, Map<String, ProcedureConfig>> map = new TreeMap<String, Map<String, ProcedureConfig>>();
        MycatConfig mycatConf = MycatServer.getInstance().getConfig();
        for (SchemaConfig schemaConf : mycatConf.getSchemas().values()) {
            Map<String, ProcedureConfig> procMap = new TreeMap<String, ProcedureConfig>(schemaConf.getProcedures());
            map.put(schemaConf.getName(), procMap);
        }
        return map;
    }
    
}
