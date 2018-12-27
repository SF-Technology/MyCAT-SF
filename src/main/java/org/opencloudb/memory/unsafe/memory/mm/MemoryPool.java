/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencloudb.memory.unsafe.memory.mm;

import javax.annotation.concurrent.GuardedBy;

/**
 * Manages bookkeeping for an adjustable-sized region of memory. This class is internal to
 * the [[MemoryManager]]. See subclasses for more details.
 *
 */
public  abstract class MemoryPool {
  /**
   *  lock [[MemoryManager]] instance, used for synchronization. We purposely erase the type
   *  to `Object` to avoid programming errors, since this object should only be used for
   *  synchronization purposes.
   */
  protected final Object lock;
  public  MemoryPool(Object lock){
    this.lock = lock;
  }

  @GuardedBy("lock")
  private  long _poolSize = 0;

  /**
   * Returns the current size of the pool, in bytes.
   */
  public final long poolSize()  {
    synchronized(lock) {
      return _poolSize;
    }
  }

  /**
   * Returns the amount of free memory in the pool, in bytes.
   */
  public long memoryFree() {
    synchronized(lock) {
     return (_poolSize - memoryUsed());
    }
  }

  /**
   * Expands the pool by `delta` bytes.
   */
  public final void incrementPoolSize(long delta) {
      assert (delta >= 0);
      synchronized(lock) {
        _poolSize += delta;
      }
  }

  /**
   * Shrinks the pool by `delta` bytes.
   */
  public final void decrementPoolSize(long delta){
      synchronized(lock) {
        assert (delta >= 0);
        assert (delta <= _poolSize);
        assert (_poolSize - delta >= memoryUsed());
        _poolSize -= delta;
      }
}

  /**
   * Returns the amount of used memory in this pool (in bytes).
   */
  protected abstract  long memoryUsed();
}
