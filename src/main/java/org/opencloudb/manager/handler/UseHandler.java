package org.opencloudb.manager.handler;

import java.nio.ByteBuffer;

import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.util.StringUtil;

/**
 * 管理端口use database 命令处理器
 * @author CrazyPig
 * @since 2017-02-09
 *
 */
public class UseHandler {
	
	public static void handle(String sql, ManagerConnection c, int offset) {
        
		String schema = sql.substring(offset).trim();
        int length = schema.length();
        
        if (length > 0) {
        	if(schema.endsWith(";")) schema=schema.substring(0,schema.length()-1);
        	schema = StringUtil.replaceChars(schema, "`", null);
        	length=schema.length();
            if (schema.charAt(0) == '\'' && schema.charAt(length - 1) == '\'') {
                schema = schema.substring(1, length - 1);
            }
        }
        
        if(MycatServer.getInstance().getConfig().getSchemas().containsKey(schema)) {
        	c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + schema + "'");
            return;
        }
        
        c.setSchema(schema);
        ByteBuffer buffer = c.allocate();
        c.write(c.writeToBuffer(OkPacket.OK, buffer));
        
    }

}
