package org.opencloudb.manager.handler;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.loader.xml.jaxb.SchemaJAXB;
import org.opencloudb.config.model.ProcedureConfig;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.util.ConfigTar;
import org.opencloudb.config.util.JAXBUtil;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.manager.parser.druid.statement.MycatCreateProcedureStatement;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.util.SplitUtil;
import org.opencloudb.util.StringUtil;

import com.alibaba.druid.sql.ast.expr.SQLCharExpr;

/**
 * 
 * @author 01169238
 * @since 1.5.3
 *
 */
public class CreateProcedureHandler {
    
    private static final Logger LOGGER = Logger.getLogger(CreateProcedureHandler.class);
    
    public static void handle(ManagerConnection c, MycatCreateProcedureStatement stmt, String sql) {

        MycatConfig mycatConf = MycatServer.getInstance().getConfig();
        mycatConf.getLock().lock();
        try {
            c.setLastOperation("create procedure " + stmt.getProcedure().getSimpleName()); // 记录操作
            String schemaName = (stmt.getSchema() == null ? c.getSchema() : StringUtil.removeBackquote(stmt.getSchema().getSimpleName()));
            
            if (schemaName == null) {
                c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "No database selected");
                return ;
            }
            
            if (!mycatConf.getUsers().get(c.getUser()).getSchemas().contains(schemaName)) {
                c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + schemaName + "'");
                return ;
            }
            
            SchemaConfig schemaConf = mycatConf.getSchemas().get(schemaName);
            if (schemaConf == null) {
                c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + schemaName + "'");
                return ;
            }
            
            /*
             * schema dataNode属性不为空, 表示该schema不是sharding schema, 不能在此schema上创建procedure或者drop procedure
             */
            if (schemaConf.getDataNode() != null) {
                c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "noSharding schema can not create or drop procedure");
                return ;
            }
            
            String procName = StringUtil.removeBackquote(stmt.getProcedure().getSimpleName());
            String upperProcName = procName.toUpperCase();
            String dataNode = ((SQLCharExpr)stmt.getDataNodes()).getText();
            // 验证procedure是否已经存在, 注意内存态的procedure命名为大写
            if (schemaConf.getProcedures().containsKey(upperProcName)) {
                c.writeErrMessage(ErrorCode.ER_SP_ALREADY_EXISTS, "procedure '" + procName + "' already exist in schema '" + schemaName + "'");
                return ;
            }
            // 验证dataNode是否存在
            String[] dataNodes = SplitUtil.split(dataNode, ',', '$', '-');
            for (String _dataNode : dataNodes) {
                if (!mycatConf.getDataNodes().containsKey(_dataNode)) {
                    c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "dataNode '" + _dataNode + "' dosen't exist");
                    return ;
                }
            }
            ProcedureConfig procConf = new ProcedureConfig(upperProcName, dataNode);
            schemaConf.getProcedures().put(upperProcName, procConf);
            SchemaJAXB schemaJAXB = JAXBUtil.toSchemaJAXB(mycatConf.getSchemas());
            // 刷新 schema.xml
            if (!JAXBUtil.flushSchema(schemaJAXB)) {
                // 出错回滚
                schemaConf.getProcedures().remove(upperProcName);
                c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "flush schema.xml fail");
                return ;
            }
            // 更新datanode, Tips: 引入的procedure有可能增加新的datanode
            schemaConf.updateDataNodesMeta();
            
            // 对配置信息进行备份
            try {
                ConfigTar.tarConfig(c.getLastOperation());
            } catch (Exception e) {
                throw new Exception("Fail to do backup.");
            }
            
            // 响应客户端
            ByteBuffer buffer = c.allocate();
            c.write(c.writeToBuffer(OkPacket.OK, buffer));
            
        } catch (Exception e) {
            c.setLastOperation("create procedure " + stmt.getProcedure().getSimpleName()); // 记录操作
            LOGGER.error(e.getMessage(), e);
            c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
        } finally {
            mycatConf.getLock().unlock();
        }
        
    }

}
