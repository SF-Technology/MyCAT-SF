/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package org.opencloudb.manager;

import org.apache.log4j.Logger;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.handler.*;
import org.opencloudb.manager.handler.CheckHandler;
import org.opencloudb.manager.handler.MycatConfigHandler;
import org.opencloudb.manager.handler.ChecksumHandler;
import org.opencloudb.net.handler.FrontendQueryHandler;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.parser.ManagerParse;
import org.opencloudb.response.KillConnection;
import org.opencloudb.response.Offline;
import org.opencloudb.response.Online;

/**
 * @author mycat
 */
public class ManagerQueryHandler implements FrontendQueryHandler {
    private static final Logger     LOGGER = Logger.getLogger(ManagerQueryHandler.class);
    private static final int        SHIFT  = 8;
    private final ManagerConnection source;
    protected Boolean               readOnly;

	public ManagerQueryHandler(ManagerConnection source) {
		this.source = source;
	}
	
	public void setReadOnly(Boolean readOnly) {
        this.readOnly = readOnly;
    }

    @Override
    public void query(String sql) {
        ManagerConnection c = this.source;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(new StringBuilder().append(c).append(sql).toString());
        }
        int rs = ManagerParse.parse(sql);
        switch (rs & 0xff) {
            case ManagerParse.SELECT:
                SelectHandler.handle(sql, c, rs >>> SHIFT);
                break;
            case ManagerParse.INSERT:
            case ManagerParse.UPDATE:
                InsertHandler.execute(c,sql);
                break;
            case ManagerParse.DELETE:
                DeleteHandler.execute(c,sql);
                break;
            case ManagerParse.SET:
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                break;
            case ManagerParse.SHOW:
                ShowHandler.handle(sql, c, rs >>> SHIFT);
                break;
            case ManagerParse.SWITCH:
                SwitchHandler.handler(sql, c, rs >>> SHIFT);
                break;
            case ManagerParse.KILL_CONN:
                KillConnection.response(sql, rs >>> SHIFT, c);
                break;
            case ManagerParse.OFFLINE:
                Offline.execute(sql, c);
                break;
            case ManagerParse.ONLINE:
                Online.execute(sql, c);
                break;
            case ManagerParse.STOP:
                StopHandler.handle(sql, c, rs >>> SHIFT);
                break;
            case ManagerParse.RELOAD:
                ReloadHandler.handle(sql, c, rs >>> SHIFT);
                break;
            case ManagerParse.ROLLBACK:
                RollbackHandler.handle(sql, c, rs >>> SHIFT);
                break;
            case ManagerParse.CLEAR:
                ClearHandler.handle(sql, c, rs >>> SHIFT);
                break;
            case ManagerParse.CHECK:
            	CheckHandler.handle(sql, c, rs >>> SHIFT);
            	break;
            case ManagerParse.MYCAT_CONFIG:
            	MycatConfigHandler.handle(c, sql, rs >>> SHIFT);
            	break;
//            case ManagerParse.CREATE:
//            	CreateHandler.handle(sql, c);
//            	break;
//            case ManagerParse.DROP:
//            	DropHandler.handle(sql, c);
//            	break;
            case ManagerParse.CHECKSUM:
            	ChecksumHandler.handle(sql, c);
            	break;
            case ManagerParse.CONFIGFILE:
                ConfFileHandler.handle(sql, c);
                break;
            case ManagerParse.LOGFILE:
                ShowServerLog.handle(sql, c);
                break;
//            case ManagerParse.LIST:
//            	ListHandler.handle(sql, c);
//            	break;
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }

}