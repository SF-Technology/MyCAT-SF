package org.opencloudb.handler;

import org.h2.util.StringUtils;
import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.monitor.H2DBMonitorManager;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.parser.util.ParseUtil;
import org.opencloudb.sqlfw.H2DBManager;
import org.opencloudb.sqlfw.SQLFirewallServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zagnix
 * @create 2016-11-08 16:58
 */

public class DeleteHandler {
    private final static Logger LOGGER =
            LoggerFactory.getLogger(DeleteHandler.class);
    public static void execute(ManagerConnection c, String sql) {
        delete(sql,c);
    }

    private static void delete(String sql, ManagerConnection c) {
        Connection dbConn = null;
        Statement stmt = null;
        ResultSet rset = null;
        boolean isSqlfwdb = false;
        boolean isMultiDel = false;
        int affectedRows = 0;

        if (sql !=null && (sql.indexOf(H2DBManager.getSqlBackListTableName()) !=-1 ||
                           sql.indexOf(H2DBManager.getSqlReporterTableName()) !=-1)){
            dbConn = H2DBManager.getH2DBManager().getH2DBConn();
        }else {
            c.writeErrMessage(ErrorCode.ER_YES,"not support delete op");
            return;
        }

        int sql_id = 0;
        String id =  ParseUtil.parseString(sql);
        SQLFirewallServer sqlFirewallServer =
                MycatServer.getInstance().getSqlFirewallServer();

        if (sql != null && id !=null && sql.indexOf("where")==-1){
            isMultiDel = true;
        }else if (sql != null && id != null && StringUtils.isNumber(id)){
            sql_id = Integer.valueOf(id);
            affectedRows = 1;
        }

        try {
            stmt = dbConn.createStatement();
            stmt.execute(sql);

         if(isMultiDel){
             ConcurrentHashMap<Integer,String> map
                     = SQLFirewallServer.getSqlBlackListMap();
             affectedRows = map.size();
             for (int key: map.keySet()) {
                 sqlFirewallServer.removeSqlfromBackList(key);
             }
         }else {
             sqlFirewallServer.removeSqlfromBackList(sql_id);
         }

         OkPacket ok = new OkPacket();
         ok.packetId = 1;
         ok.affectedRows = affectedRows;
         ok.serverStatus = 2;
         ok.write(c);

        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
            c.writeErrMessage(ErrorCode.ER_YES, e.getMessage());
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (rset != null) {
                    rset.close();
                }
            } catch (SQLException e) {
                LOGGER.error(e.getMessage());
                c.writeErrMessage(ErrorCode.ER_YES, e.getMessage());
            }
        }
    }
}
