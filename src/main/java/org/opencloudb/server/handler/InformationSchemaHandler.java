package org.opencloudb.server.handler;

import java.nio.ByteBuffer;
import java.util.List;

import org.opencloudb.config.Fields;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.util.ByteUtil;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;

/**
 * 
 * INFORMATION_SCHEMA查询语句处理类, 目前做的只是拦截这类sql, 防止其继续发送解析报错<br> 
 * 这里通过解析select的字段, 模拟返回空的结果集给前端<br>
 * TODO 后面考虑处理这类语句的路由<br>
 * 
 * sql包括:
 * <pre>
 * 1. SELECT QUERY_ID, SUM(DURATION) AS SUM_DURATION FROM INFORMATION_SCHEMA.PROFILING GROUP BY QUERY_ID;
 * 2. SELECT STATE AS `Status`, ROUND(SUM(DURATION),7) AS `Duration`, CONCAT(ROUND(SUM(DURATION)/*100,3), '%') AS `Percentage` FROM INFORMATION_SCHEMA.PROFILING WHERE QUERY_ID= GROUP BY STATE ORDER BY SEQ;
 * </pre>
 * 
 * @author crazypig
 * @since 2016-09-20
 *
 */
public class InformationSchemaHandler {
	
	private static final String INFORMATION_SCHMEA = "information_schema";
	
	public static void handle(String sql, ServerConnection c) {
		
		ByteBuffer buffer = c.allocate();
		
		String[] fieldNames = getFieldNames(sql);
		
		int fieldCount = fieldNames.length;
		
		// write header
        ResultSetHeaderPacket header = PacketUtil.getHeader(fieldCount);
        byte packetId = header.packetId;
        buffer = header.write(buffer, c, true);
		
        // write field packet
        FieldPacket[] fields = new FieldPacket[fieldCount];
        for(int i = 0; i < fields.length; i++) {
        	fields[i] = PacketUtil.getField(fieldNames[i], Fields.FIELD_TYPE_VAR_STRING);
        	fields[i].packetId = ++packetId;
        	buffer = fields[i].write(buffer, c, true);
        }
        
        // write eof
        EOFPacket eof = new EOFPacket();
        eof.packetId = ++packetId;
        buffer = eof.write(buffer, c, true);
        
        // write row, mock return
        packetId = writeRowPacks(fieldNames, buffer, c, packetId);
        
        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c, true);
        
        c.write(buffer);
        
	}
	
	private static byte writeRowPacks(String[] fieldNames, ByteBuffer buffer, ServerConnection c, byte packetId) {
		if(fieldNames.length == 2) {
			if("QUERY_ID".equalsIgnoreCase(fieldNames[0]) && "SUM_DURATION".equalsIgnoreCase(fieldNames[1])) {
				/*
				 * 处理SELECT QUERY_ID, SUM(DURATION) AS SUM_DURATION FROM INFORMATION_SCHEMA.PROFILING GROUP BY QUERY_ID;
				 * 因为后面的sql查询要依赖这个查询结果
				 */
				RowDataPacket row = new RowDataPacket(2);
				row.packetId = ++packetId;
				row.add(ByteUtil.getBytes("0", c.getCharset())); // QUERY_ID
				row.add(ByteUtil.getBytes("0.0001", c.getCharset())); // SUM_DURATION
				buffer = row.write(buffer, c, true);
			}
		}
		return packetId;
	}
	
	/**
	 * 从sql语句中解析出select字段名,以数组形式返回
	 * @param sql
	 * @return
	 */
	public static String[] getFieldNames(String sql) {
		boolean lowercase = false;
		if(sql.contains(INFORMATION_SCHMEA)) {
			lowercase = true;
		}
		String tsql = truncateSqlUtilTable(sql, lowercase);
		MySqlStatementParser parser = new MySqlStatementParser(tsql);
		SQLStatement stmt = parser.parseStatement();
		SQLSelectStatement selStmt = (SQLSelectStatement) stmt;
		MySqlSelectQueryBlock queryBlock = (MySqlSelectQueryBlock) selStmt.getSelect().getQuery();
		List<SQLSelectItem> selectItemList = queryBlock.getSelectList();
		int size = selectItemList.size();
		String[] fieldNames = new String[size];
		for(int i = 0; i < size; i++) {
			SQLSelectItem selectItem = selectItemList.get(i);
			String alias = selectItem.getAlias();
			if(alias != null) { // 取别名作为返回列名
				fieldNames[i] = alias;
			} else { // 取真实列名作为返回列名
				if(selectItem.getExpr() instanceof SQLAllColumnExpr) {
					fieldNames[i] = "*"; // TODO 是否考虑返回真实的所有列?
				} else if(selectItem.getExpr() instanceof SQLIdentifierExpr) {
					fieldNames[i] = ((SQLIdentifierExpr)selectItem.getExpr()).getName();
				}
			}
		}
		return fieldNames;
	}
	
	/**
	 * 截断表名后面的条件(条件有可能不全,使用druid解析可能出错), 留下的sql串格式为:<br>
	 * select xxx,xxx,... from information_schema.COLUMNS
	 * @param sql
	 * @param lowercase
	 * @return
	 */
	private static String truncateSqlUtilTable(String sql, boolean lowercase) {
		int index = -1;
		if(lowercase) {
			index = sql.indexOf(INFORMATION_SCHMEA) + INFORMATION_SCHMEA.length();
		} else {
			index = sql.indexOf(INFORMATION_SCHMEA.toUpperCase()) + INFORMATION_SCHMEA.length();
		}
		char stopChar = ' ';
		char[] charArrOfSql = sql.toCharArray();
		while(charArrOfSql[index] != stopChar) {
			index++;
		}
		String tsql = sql.substring(0, index);
		return tsql;
	}
	
}
