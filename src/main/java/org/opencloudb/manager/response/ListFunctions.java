package org.opencloudb.manager.response;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;
import org.opencloudb.config.Fields;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.route.function.AbstractPartitionAlgorithm;
import org.opencloudb.util.StringUtil;

public class ListFunctions {
	private static final Logger LOGGER = Logger.getLogger(ListFunctions.class);
	
	private static final int FIELD_COUNT = 3;
	private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();

	static {
		int i = 0;
		byte packetId = 1;

		header.packetId = packetId++;

		fields[i] = PacketUtil.getField("functionName", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = packetId++;

		fields[i] = PacketUtil.getField("class", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = packetId++;

		fields[i] = PacketUtil.getField("properties", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = packetId++;

		eof.packetId = packetId++;
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
		Map<String, AbstractPartitionAlgorithm> functions = MycatServer.getInstance().getConfig().getFunctions();
		for (String name : functions.keySet()){
			AbstractPartitionAlgorithm function = functions.get(name);
			
			RowDataPacket row = new RowDataPacket(FIELD_COUNT);
			
			row.add(StringUtil.encode(name, c.getCharset()));
			row.add(StringUtil.encode(function.getClass().getName(), c.getCharset()));
			row.add(StringUtil.encode(acquireProperties(function).toString(), c.getCharset()));
			
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
	
	public static Map<String, Object> acquireProperties(AbstractPartitionAlgorithm function){
		Map<String, Object> properties = new HashMap<String, Object>(); // key=属性，value=默认值
		
		try {
			BeanInfo beanInfo = Introspector.getBeanInfo(function.getClass());
			PropertyDescriptor[] pds = beanInfo.getPropertyDescriptors();
			for (PropertyDescriptor descriptor : pds) {
				Method getMethod = descriptor.getReadMethod();
				Method setMethod = descriptor.getWriteMethod();
				
				if (getMethod != null && setMethod != null) {
					Object value = getMethod.invoke(function);
					properties.put(descriptor.getName(), value);
				}
			}
		} catch (IntrospectionException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			LOGGER.error("Fail to get properties.", e);
			throw new RuntimeException(e);
		}
		
		return properties;
	}
}
