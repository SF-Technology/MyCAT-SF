package org.opencloudb.memory.unsafe.ringbuffer;



import org.opencloudb.memory.unsafe.array.LongArray;
import org.opencloudb.memory.unsafe.memory.mm.DataNodeMemoryManager;
import org.opencloudb.memory.unsafe.memory.mm.MemoryConsumer;

import java.io.IOException;

/**
 *
 * 环形buffer 待实现，
 */
public class RingBuffer extends MemoryConsumer {

	private LongArray array;
	private int in;
	private int out;
	private int size;
	
	public RingBuffer(DataNodeMemoryManager dataNodeMemoryManager, int pageSize){
		super(dataNodeMemoryManager,pageSize);
	}

	@Override
	public long spill(long size, MemoryConsumer trigger) throws IOException {
		return 0;
	}

}
