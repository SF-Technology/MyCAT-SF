package org.opencloudb.response;

import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.stat.QueryResultDispatcher;
import org.opencloudb.trace.SqlTraceDispatcher;

/**
 * 关闭/打开  统计模块
 * 
 * reload @@sqltrace=close;
 * reload @@sqltrace=open;
 * 
 * @author zhouhao
 *
 */
public class ReloadSqlTrace {
	
    public static void execute(ManagerConnection c, String trace) {
    	
    	boolean isOk = false;
    	
    	if ( trace != null ) {
    	
    		if ( trace.equalsIgnoreCase("OPEN") ) {
    			isOk = SqlTraceDispatcher.open();
    			
    		} else if ( trace.equalsIgnoreCase("CLOSE") ) {
    			isOk = SqlTraceDispatcher.close();
    		}
	    	
    		StringBuffer sBuffer = new StringBuffer(35);
    		sBuffer.append( "Set sql trace module isclosed=").append( trace ).append(",");
    		sBuffer.append( (isOk == true ? " to succeed" : " to fail" ));
    		sBuffer.append( " by manager. ");
	        
	        OkPacket ok = new OkPacket();
	        ok.packetId = 1;
	        ok.affectedRows = 1;
	        ok.serverStatus = 2;
	        ok.message = sBuffer.toString().getBytes();
	        ok.write(c);
    	}
    }


}
