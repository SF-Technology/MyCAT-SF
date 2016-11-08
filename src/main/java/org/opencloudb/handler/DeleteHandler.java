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




        if (sql !=null && (sql.indexOf(H2DBManager.getSqlBackListTableName()) !=-1 ||
                           sql.indexOf(H2DBManager.getSqlReporterTableName()) !=-1)){
            dbConn = H2DBManager.getH2DBManager().getH2DBConn();
        }else {
            dbConn = H2DBMonitorManager.
                    getH2DBMonitorManager().getH2DBMonitorConn();
        }

        int sql_id = 0;
        String id =  ParseUtil.parseString(sql);

        if (sql != null && id !=null && StringUtils.isNumber(id)){
            sql_id = Integer.valueOf(id);
        }else {
            c.writeErrMessage(ErrorCode.ER_YES, "sql id must be number .");
            return;
        }

        try {
            stmt = dbConn.createStatement();
            stmt.execute(sql);


            SQLFirewallServer sqlFirewallServer =
                    MycatServer.getInstance().getSqlFirewallServer();
            if(sqlFirewallServer.removeSqlfromBackList(sql_id)){
                OkPacket ok = new OkPacket();
                ok.packetId = 1;
                ok.affectedRows = 1;
                ok.serverStatus = 2;
                ok.write(c);
            }else {
                c.writeErrMessage(ErrorCode.ER_YES, "delete blacklist failed.");
            }
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
