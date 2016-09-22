package org.opencloudb.server.handler;

import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * @author crazypig
 * @since 2016-09-20
 *
 */
public class InformationSchemaHandlerTest {
	
	@Test
	public void testGetFieldNames() {
		
		String sql = "SELECT A as a, B as b FROM INFORMATION_SCHEMA.PROFILING WHERE QUERY_ID = 1";
		String[] fieldNames = InformationSchemaHandler.getFieldNames(sql);
		Assert.assertArrayEquals(new String[]{"a", "b" }, fieldNames);
		
		sql = "SELECT A, B FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'hotnews'";
		fieldNames = InformationSchemaHandler.getFieldNames(sql);
		Assert.assertArrayEquals(new String[]{"A" , "B" }, fieldNames);
	}

}
