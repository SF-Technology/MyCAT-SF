package org.opencloudb.manager.handler;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.MycatManageStatementParser;
import org.opencloudb.manager.parser.druid.statement.MycatDumpStatement;
import org.opencloudb.manager.response.DumpConfigSql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.parser.ParserException;
import com.google.common.base.Strings;

/**
 * mycat_config dump 命令处理器
 * @author 01169238
 * @since 1.5.3
 *
 */
public class MycatConfigDumpHandler {
    
    private static final int DEFAULT_DUMP_BUFFER_SIZE = 4096;
    private static final Logger LOGGER = Logger.getLogger(MycatConfigDumpHandler.class);

    public static void handle(String sql, ManagerConnection c) {
        MycatManageStatementParser parser = new MycatManageStatementParser(sql);
        try {
            SQLStatement stmt = parser.parseStatement();
            if (stmt instanceof MycatDumpStatement) {
                MycatDumpStatement dumpStmt = (MycatDumpStatement) stmt;
                String contentToFile = null;
                if (dumpStmt.isDumpIntoFile()) {
                    contentToFile = dumpStmt.getIntoFile().getText();
                    // 验证文件是否已经存在
                    if (!validate(c, contentToFile)) {
                        return ;
                    }
                }
                switch (dumpStmt.getTarget()) {
                    case ALL:
                        handleDumpAll(c, contentToFile);
                        break;
                    case ALL_TABLES:
                        handleDumpTables(dumpStmt, c, true, contentToFile);
                        break;
                    case SCHEMAS:
                        handleDumpSchemas(dumpStmt, c, contentToFile);
                        break;
                    case TABLES:
                        handleDumpTables(dumpStmt, c, false, contentToFile);
                        break;
                    default:
                        c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED,
                                "Unsupport statment : " + sql);
                        break;
                }
            } else {
                c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "Unsupport statment : " + sql);
            }
        } catch (ParserException e) {
            c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
        }
    }
    
    /**
     * mycat_config dump all的处理
     * @param c
     * @param contentToFile 如果不为空, 表示将dump出来的内容保存到本地路径文件, contentToFile表示保存的文件名
     * @throws IOException
     */
    private static void handleDumpAll(ManagerConnection c, String contentToFile) throws IOException {
        StringBuilder content = new StringBuilder(DEFAULT_DUMP_BUFFER_SIZE * 2);
        MycatConfigDumper.dumpAll(content);
        DumpConfigSql.response(c, content, contentToFile);
    }
    
    /**
     * mycat_config dump schemas的处理
     * @param stmt
     * @param c
     * @param contentToFile 如果不为空, 表示将dump出来的内容保存到本地路径文件, contentToFile表示保存的文件名
     * @throws IOException
     */
    private static void handleDumpSchemas(MycatDumpStatement stmt, ManagerConnection c, String contentToFile) throws IOException {
        MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
        Map<String, List<String>> tables = new TreeMap<String, List<String>>();
        for (SQLExpr item : stmt.getItems()) {
            String schemaName = ((SQLName) item).getSimpleName();
            // 判断dump schemas 后面带的schema是否都存在
            if (mycatConfig.getSchemas().get(schemaName) == null) {
                c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + schemaName + "'");
                return ;
            }
            tables.put(schemaName, new ArrayList<String>(mycatConfig.getSchemas().get(schemaName).getTables().keySet()));
        }
        StringBuilder content = new StringBuilder(DEFAULT_DUMP_BUFFER_SIZE);
        MycatConfigDumper.dump(content, tables, true);
        DumpConfigSql.response(c, content, contentToFile);
    }
    
    /**
     * mycat_config dump tables 和 dump all_tables的处理
     * @param stmt
     * @param c
     * @param all 是否dump ALL_TABLES
     * @param contentToFile 如果不为空, 表示将dump出来的内容保存到本地路径文件, contentToFile表示保存的文件名
     * @throws IOException 
     */
    private static void handleDumpTables(MycatDumpStatement stmt, ManagerConnection c, boolean all, String contentToFile) throws IOException {
        String schemaName = c.getSchema();
        // 判断schema是否为空
        if (Strings.isNullOrEmpty(schemaName)) {
            c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
            return ;
        }
        MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
        SchemaConfig schema = mycatConfig.getSchemas().get(schemaName);
        if (schema == null) {
            c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + schemaName + "'");
            return ;
        }
        Map<String, List<String>> tableMap = new TreeMap<String, List<String>>();
        if (all) {
            tableMap.put(schemaName, new ArrayList<String>(mycatConfig.getSchemas().get(schemaName).getTables().keySet()));
        } else {
            List<String> tables = new ArrayList<String>();
            for (SQLExpr item : stmt.getItems()) {
                String tableName = ((SQLName) item).getSimpleName().toUpperCase();
                // 检查dump tables 指定的table是否都存在
                if (schema.getTables().get(tableName) == null) {
                    c.writeErrMessage(ErrorCode.ER_BAD_TABLE_ERROR, "Unknown table '" + tableName + "' in " + schemaName);
                    return ;
                }
                tables.add(tableName);
            }
            tableMap.put(schemaName, tables);
        }
        StringBuilder content = new StringBuilder(DEFAULT_DUMP_BUFFER_SIZE);
        MycatConfigDumper.dump(content, tableMap, false);
        DumpConfigSql.response(c, content, contentToFile);
    }
    
    private static boolean validate(ManagerConnection c, String fileName) throws IOException, URISyntaxException {
        Path basePath = Paths.get(SystemConfig.class.getClassLoader().getResource("").toURI());
        Path dirPath = Paths.get(basePath.toString(), SystemConfig.getDumpFileFolder());
        // 如果目录不存在, 创建目录
        if (!Files.exists(dirPath)) {
            Files.createDirectory(dirPath);
            return true;
        }
        Path filePath = Paths.get(dirPath.toString(), fileName);
        // 如果文件已存在, 返回错误信息
        if (Files.exists(filePath)) {
            c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "dump file : " + fileName + " already exist!");
            return false;
        }
        return true;
    }
    
}
