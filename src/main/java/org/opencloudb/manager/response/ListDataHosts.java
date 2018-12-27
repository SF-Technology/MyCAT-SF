package org.opencloudb.manager.response;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.opencloudb.MycatServer;
import org.opencloudb.backend.PhysicalDBPool;
import org.opencloudb.backend.PhysicalDatasource;
import org.opencloudb.config.Fields;
import org.opencloudb.config.model.DBHostConfig;
import org.opencloudb.config.model.DataHostConfig;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.util.StringUtil;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * list datahosts 语句响应
 * @author CrazyPig
 * @since 2017-02-09
 *
 */
public class ListDataHosts {
	
	private static final int FIELD_COUNT = 9;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    
    static {
    	int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("dhName", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("maxCon", Fields.FIELD_TYPE_INT24);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("minCon", Fields.FIELD_TYPE_INT24);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("balance", Fields.FIELD_TYPE_INT24);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("dbType", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("dbDriver", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("switchType", Fields.FIELD_TYPE_INT24);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("writeHosts", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("readHosts", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        eof.packetId = ++packetId;
    }
	
	public static void response(ManagerConnection c) {
		
		ByteBuffer buffer = c.allocate();

		// write header
		buffer = header.write(buffer, c, true);

		// write fields
		for (FieldPacket field : fields) {
			buffer = field.write(buffer, c, true);
		}

		// write eof
		buffer = eof.write(buffer, c, true);

		byte packetId = eof.packetId;
		
		// write rows
		Map<String, PhysicalDBPool> dataHosts = MycatServer.getInstance().getConfig().getDataHosts();
		for(String dhName : new TreeSet<String>(dataHosts.keySet())) {
			PhysicalDBPool dbPool = dataHosts.get(dhName);
			List<PhysicalDatasource> writeSources = new ArrayList<PhysicalDatasource>();
			List<PhysicalDatasource> readSources = new ArrayList<PhysicalDatasource>();
			for(PhysicalDatasource source : dbPool.getAllDataSources()) {
				if(!source.isReadNode()) {
					writeSources.add(source);
				} else {
					readSources.add(source);
				}
			}
			RowDataPacket row = new RowDataPacket(FIELD_COUNT);
			row.add(StringUtil.encode(dhName, c.getCharset()));
			PhysicalDatasource writeSource = writeSources.get(0);
			DataHostConfig dhConf = writeSource.getHostConfig();
			row.add(StringUtil.encode(String.valueOf(dhConf.getMaxCon()), c.getCharset()));
			row.add(StringUtil.encode(String.valueOf(dhConf.getMinCon()), c.getCharset()));
			row.add(StringUtil.encode(String.valueOf(dhConf.getBalance()), c.getCharset()));
			row.add(StringUtil.encode(dhConf.getDbType(), c.getCharset()));
			row.add(StringUtil.encode(dhConf.getDbDriver(), c.getCharset()));
			row.add(StringUtil.encode(String.valueOf(dhConf.getSwitchType()), c.getCharset()));
			
			row.add(StringUtil.encode(getWRHostJsonStr(writeSources), c.getCharset()));
			row.add(StringUtil.encode(getWRHostJsonStr(readSources), c.getCharset()));
			
			row.packetId = ++packetId;
			buffer = row.write(buffer, c, true);
		}
		
		// write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c, true);

        // post write
        c.write(buffer);
	}
	
	private static String getWRHostJsonStr(List<PhysicalDatasource> sources) {
		JSONArray jsonArr = new JSONArray();
		for(PhysicalDatasource souces : sources) {
			DBHostConfig dbHostConf = souces.getConfig();
			JSONObject obj = new JSONObject(true);
			obj.put("host", dbHostConf.getHostName());
			obj.put("url", dbHostConf.getUrl());
			obj.put("user", dbHostConf.getUser());
			obj.put("password", dbHostConf.getPassword());
			jsonArr.add(obj);
		}
		return jsonArr.toJSONString();
	}

}
