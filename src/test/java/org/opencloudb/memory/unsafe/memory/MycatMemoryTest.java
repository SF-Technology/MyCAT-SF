package org.opencloudb.memory.unsafe.memory;


import org.junit.Test;
import org.opencloudb.memory.MyCatMemory;
import org.opencloudb.memory.unsafe.Platform;

/**
 * Created by zagnix on 2016/6/12.
 */
public class MycatMemoryTest {

    /**
     * -Xmx1024m -XX:MaxDirectMemorySize=1G
     */
    @Test
    public void testMycatMemory() throws NoSuchFieldException, IllegalAccessException {
        MyCatMemory myCatMemory = new MyCatMemory();
        System.out.println(myCatMemory.getResultSetBufferSize());
        System.out.println(Platform.getMaxHeapMemory());
        System.out.println(Platform.getMaxDirectMemory());
    }

}
