package org.opencloudb.handler;

import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.monitor.H2DBMonitorManager;
import org.opencloudb.net.mysql.OkPacket;
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
public class InsertHandler {
    private final static Logger LOGGER =
            LoggerFactory.getLogger(InsertHandler.class);
    public static void execute(ManagerConnection c, String sql) {
        insert(sql,c);
    }
    private static void insert(String sql, ManagerConnection c) {
        Connection dbConn = null;
        Statement stmt = null;
        ResultSet rset = null;

        if (sql !=null && (sql.indexOf(H2DBManager.getSqlBackListTableName()) !=-1)){
            dbConn = H2DBManager.getH2DBManager().getH2DBConn();
        }else {
            c.writeErrMessage(ErrorCode.ER_YES,"not support insert op !");
            return;
        }

        try {
            stmt = dbConn.createStatement();
            stmt.executeUpdate(sql);
            OkPacket ok = new OkPacket();
            ok.packetId = 1;
            ok.affectedRows = 1;
            ok.write(c);

            SQLFirewallServer sqlFirewallServer =
                    MycatServer.getInstance().getSqlFirewallServer();

            sqlFirewallServer.loadSQLBlackList();

        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
            c.writeErrMessage(ErrorCode.ER_YES, e.getMessage());
        }finally {

            try {
                if(stmt !=null){
                    stmt.close();
                }
                if (rset !=null){
                    rset.close();
                }
            } catch (SQLException e) {
                LOGGER.error(e.getMessage());
                c.writeErrMessage(ErrorCode.ER_YES, e.getMessage());
            }

        }
    }
}
