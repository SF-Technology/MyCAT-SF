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
import org.opencloudb.manager.parser.druid.statement.MycatDropProcedureStatement;
import org.opencloudb.net.mysql.OkPacket;

/**
 * 
 * @author 01169238
 * @since 1.5.3
 *
 */
public class DropProcedureHandler {
    
    private static final Logger LOGGER = Logger.getLogger(DropProcedureHandler.class);
    
    public static void handle(ManagerConnection c, MycatDropProcedureStatement stmt, String sql) {
        
        String procName = stmt.getProcedure().getSimpleName();
        String upperProcName = procName.toUpperCase();
        MycatConfig mycatConf = MycatServer.getInstance().getConfig();
        
        mycatConf.getLock().lock();
        try {
            c.setLastOperation("drop procedure " + stmt.getProcedure().getSimpleName()); // 记录操作
            // 获取当前schema
            String schemaName = c.getSchema();
            // (可选)检查schema是否存在
            if (!mycatConf.getSchemas().containsKey(schemaName)) {
                c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "schema '" + schemaName + "' dosen't exist");
                return ;
            }
            SchemaConfig schemaConf = mycatConf.getSchemas().get(schemaName);
            /*
             * schema dataNode属性不为空, 表示该schema不是sharding schema, 不能在此schema上创建procedure或者drop procedure
             */
            if (schemaConf.getDataNode() != null) {
                c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "noSharding schema can not create or drop procedure");
                return ;
            }
            // 检查存储过程是否存在, 注意在内存态中procedure命名总是保存为大写
            ProcedureConfig procConf = schemaConf.getProcedures().get(upperProcName);
            if (procConf == null) {
                c.writeErrMessage(ErrorCode.ER_SP_DOES_NOT_EXIST, "procedure '" + procName + "' doesn't exist in schema '" + schemaName + "'");
                return ;
            }
            schemaConf.getProcedures().remove(upperProcName);
            // 刷新schema.xml
            SchemaJAXB schemaJAXB = JAXBUtil.toSchemaJAXB(mycatConf.getSchemas());
            if (!JAXBUtil.flushSchema(schemaJAXB)) {
                // 出错回滚
                schemaConf.getProcedures().put(upperProcName, procConf);
                c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "flush schema.xml fail");
                return ;
            }
            
            // 重新更新SchemaConfig datanode集合
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
            
        } catch(Exception e) {
            c.setLastOperation("drop procedure " + stmt.getProcedure().getSimpleName()); // 记录操作
            LOGGER.error(e.getMessage(), e);
            c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
        } finally {
            mycatConf.getLock().unlock();
        }
    }

}
