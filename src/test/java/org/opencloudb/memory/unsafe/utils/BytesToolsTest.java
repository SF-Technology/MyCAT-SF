package org.opencloudb.memory.unsafe.utils;

import org.junit.Assert;
import org.junit.Test;

import java.io.UnsupportedEncodingException;

/**
 * @author zagnix
 * @create 2016-10-13 16:08
 */

public class BytesToolsTest {
    @Test
    public void testDouble2Bytes() throws UnsupportedEncodingException {
        double val1 = 301353.077777;
        String dstr = Double.toString(val1);
        byte [] bytes =  "301353.077777".getBytes("US-ASCII");
        Assert.assertArrayEquals(bytes,BytesTools.double2Bytes(val1,6));

        byte [] bytes1 =  "301353.07778".getBytes("US-ASCII");
        Assert.assertArrayEquals(bytes1,BytesTools.double2Bytes(val1,5));

        double val2 = 301353.0000;

        byte [] bytes2 =  "301353.0000".getBytes("US-ASCII");
        Assert.assertArrayEquals(bytes2,BytesTools.double2Bytes(val2,4));

        byte [] bytes3 =  "301353.00".getBytes("US-ASCII");
        Assert.assertArrayEquals(bytes3,BytesTools.double2Bytes(val2,2));

        double va = 0.0/0.0;
        byte [] bytes11 = BytesTools.double2Bytes(va);
        System.out.println(new String(bytes11));
        Assert.assertEquals(true, Double.isNaN(BytesTools.getDouble(bytes11)));
    }
}
