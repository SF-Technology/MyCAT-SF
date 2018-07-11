package org.opencloudb.trace;/*
* 文 件 名: ${file_name}
* 版 权: xxx., Ltd. Copyright 2015-2018, All rights reserved
* 描 述: &lt;描述&gt;
* 修 改 人:01371805
* 修改时间: 2018/6/27
* 跟踪单号: &lt;跟踪单号&gt;
* 修改单号: &lt;修改单号&gt;
* 修改内容:&lt;修改内容&gt;
*/

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.Logger;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.mysql.nio.MySQLConnection;
import org.opencloudb.net.AbstractConnection;
import org.opencloudb.net.FrontendConnection;
import org.opencloudb.server.ServerConnection;

/**
 * SQL执行路由
 *
 * @author 01371805
 * @version [版本号, 2018/6/27]
 */
public class SqlTraceDispatcher {

  // 默认关闭SqlTrace
  private final static AtomicBoolean isClosed = new AtomicBoolean(true);

  private static final Logger LOGGER = Logger.getLogger(SqlTraceDispatcher.class);

  public static boolean close() {
    if (isClosed.compareAndSet(false, true)) {
      return true;
    }
    return false;
  }

  public static boolean open() {
    if (isClosed.compareAndSet(true, false)) {
      return true;
    }
    return false;
  }
  // frontId -> backIds
  private static Map<Long,List<Long>> connectionRelations = new ConcurrentHashMap<>();

  private static Map<Long,SqlTraceInfo> backendMapFrontRelations = new ConcurrentHashMap<>();

  public static void traceFrontConn(AbstractConnection frontConn,BackendConnection backConn){
    if ( isClosed.get() ) {
      return;
    }
    try {
      //如果前端有后端需要打印后端使用的信息
      SqlTraceInfo frontTraceInfo = new SqlTraceInfo(frontConn);
      String executeSql = "";
      if(frontConn instanceof ServerConnection){
        ServerConnection serverConnection = ((ServerConnection) frontConn);
        executeSql = serverConnection.getExecuteSql();
      }
      SqlTraceInfo backTraceInfo = null;
      if(backConn != null){
        backTraceInfo = new SqlTraceInfo(backConn);
      }

      String msg = frontTraceInfo.toString() + " executeSql :" + executeSql
          + " bind back connection:" + (backTraceInfo == null ? "null" : backTraceInfo.toString());
      LOGGER.info(msg);
    }catch (Exception e){
      LOGGER.error(e);
    }

  }

  public static void bindBackendConn(FrontendConnection frontConn,
      BackendConnection backConn) {
    if ( isClosed.get() ) {
      return;
    }
    try {
      long frontId = 0;
      String executeSql = "";
      if(frontConn instanceof ServerConnection){
        ServerConnection serverConnection = ((ServerConnection) frontConn);
        frontId = serverConnection.getId();
        executeSql = serverConnection.getExecuteSql();
      }
      long backId = 0;
      if(backConn instanceof MySQLConnection){
        MySQLConnection mySQLConnection = ((MySQLConnection) backConn);
        backId = mySQLConnection.getId();

      }
      List<Long> backIds = connectionRelations.get(frontId);
      if(backIds == null){
        backIds = new ArrayList<>();
      }
      backIds.add(backId);

      //打印绑定信息(前端和后端信息)
      SqlTraceInfo frontTraceInfo = new SqlTraceInfo(frontConn);
      SqlTraceInfo backTraceInfo = new SqlTraceInfo(backConn);
      backendMapFrontRelations.put(backId, frontTraceInfo);
      //组装
      String msg = frontTraceInfo.toString() + " executeSql :" + executeSql
          + " bind back connection " + backTraceInfo.toString();
      LOGGER.info(msg);
    }catch (Exception e){
      LOGGER.error(e);
    }
  }

  public static void traceBackendConn(BackendConnection backConn,String sqlType){
    if ( isClosed.get() ) {
      return;
    }
    try {
      //后端连接信息 打印前端连接信息
      SqlTraceInfo backTraceInfo = new SqlTraceInfo(backConn);
      long backId = 0;
      if(backConn instanceof MySQLConnection){
        MySQLConnection mySQLConnection = ((MySQLConnection) backConn);
        backId = mySQLConnection.getId();
      }
      SqlTraceInfo frontTraceInfo = backendMapFrontRelations.get(backId);

      //组装
      String msg = backTraceInfo.toString() + " receive sql type :" + sqlType
          + " bind front conn " + (frontTraceInfo == null ? "null" : frontTraceInfo.toString());
      LOGGER.info(msg);
    }catch (Exception e){
      LOGGER.error(e);
    }
  }

  public static void traceBackendConn(FrontendConnection frontConn, BackendConnection backConn,
      String sqlType) {
    if ( isClosed.get() ) {
      return;
    }
    try {
      //后端连接信息 打印前端连接信息
      SqlTraceInfo backTraceInfo = new SqlTraceInfo(backConn);
      long backId = 0;
      if(backConn instanceof MySQLConnection){
        MySQLConnection mySQLConnection = ((MySQLConnection) backConn);
        backId = mySQLConnection.getId();
      }
      SqlTraceInfo frontTraceInfo = new SqlTraceInfo(frontConn);

      //组装
      String msg = backTraceInfo.toString() + " receive sql type :" + sqlType
          + " bind front conn " + frontTraceInfo.toString();
      LOGGER.info(msg);
    }catch (Exception e){
      LOGGER.error(e);
    }
  }

}
