package org.opencloudb.sqlfw;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * SQL Record  Cache implement
 *
 * @author zagnix
 * @version 1.0
 * @create 2016-10-20 15:27
 */

public class SQLLRUCacheImpl<K,V extends H2DBInterface> implements ILRUCache<K, V>  {

    private final static Logger LOGGER = LoggerFactory.getLogger(SQLLRUCacheImpl.class);

    /**
     * value 存放在内存中
     */
    private final LinkedHashMap<K,V> lruMap;

    /**
     * value 存放在内存中
     */
    private final LinkedHashMap<K,TTLValue> ttlMap;

    /**
     *同步
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock rLock = lock.readLock();
    private final Lock wLock = lock.writeLock();

    private final Set<K> keysToH2Db = new HashSet<K>();

    private final long DEFAULT_TIMEOUT;

    public SQLLRUCacheImpl(final long timeout){
        lruMap = new LinkedHashMap<K,V>(0, 0.75f,true);
        ttlMap = new LinkedHashMap<K,TTLValue>(0, 0.75f,true);
        this.DEFAULT_TIMEOUT = timeout;
    }

    @Override
    public void put(K key, V value, long timeout) {

        Collection<V> values = lazyWillToDB();

        if (values != null && values.contains(value)) {
            values.remove(value);
        }

        lruMap.put(key, value);
        TTLValue ttl = new TTLValue(System.currentTimeMillis(),DEFAULT_TIMEOUT);
        ttlMap.put(key,ttl);

        if (values != null && values.size() > 0) {

            if (LOGGER.isDebugEnabled()) {
                int size = values.size();
                LOGGER.debug("update asynchronously row size :  " + size);
            }
            //update H2DB asynchronously
            SQLFirewallServer.getUpdateH2DBService().
                    execute(new Task<V>(values,SQLFirewallServer.OP_UPATE));
        }
    }

    @Override
    public void put(K key, V value) {
        this.put(key,value,DEFAULT_TIMEOUT);
    }


    /**
     * 先从map取元素。
     * 如果不在map中，则从H2DBz中查询
     */
    @Override
    public V get(K key) {
        try {

            rLock.lock();
            TTLValue ttl = ttlMap.get(key);

            if (ttl != null) {
                ttl.lastAccessedTimestamp.set(System.currentTimeMillis());
            }

           V value = lruMap.get(key);

            if (value == null){
                String s = key.toString();

                if (LOGGER.isDebugEnabled()){
                    LOGGER.debug("get key  :" + key);
                }

                value = (V) value.query(s);
            }

            return value;

        } finally {
            rLock.unlock();
        }
    }

    /**
     * 根据key移除元素
     * @param key the key of the cached resource
     * @return
     * @throws IOException
     */
    @Override
    public V remove(K key) throws IOException {
        try {
            wLock.lock();
            ttlMap.remove(key);
            V value = lruMap.remove(key);
            if (value != null) {
                value.delete();
            }
            return value;
        } finally {
            wLock.unlock();
        }
    }

    @Override
    public void removeAll() throws IOException {
        try {
            wLock.lock();
            Collection<V> valuesToClose = new HashSet<V>();
            valuesToClose.addAll(lruMap.values());
            if (valuesToClose != null && valuesToClose.size() > 0) {
                // close resource synchronously
                for(V v : valuesToClose) {
                    v.delete();
                }
            }
            lruMap.clear();
            ttlMap.clear();
        } finally {
            wLock.unlock();
        }
    }

    @Override
    public int size() {
        try {
            rLock.lock();
            return lruMap.size();
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Task 主要将Key对应的value超时写入H2DB中
     * @param <V>
     */
    private static class Task<V extends H2DBInterface> implements Runnable{
        private  Collection<V> valuesToH2DB = null;
        private int op = 0;

        public Task( Collection<V> cols,int op){
            this.valuesToH2DB = cols;
            this.op = op;
        }

        @Override
        public void run() {

            int size = valuesToH2DB.size();

            switch (this.op){
                case SQLFirewallServer.OP_UPATE:
                    for(V v : valuesToH2DB) {
                        if (v != null) {
                            v.update();
                        }
                    }
                    break;
                case SQLFirewallServer.OP_DEL:
                    for(V v : valuesToH2DB) {
                        if (v != null) {
                            v.delete();
                        }
                    }
                    break;
                 default:
                     LOGGER.warn("op error");
                     break;
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("ResourceCloser closed " + size + (size > 1 ? " resources.":" resource."));
            }
        }
    }


    private static class TTLValue {

        private AtomicLong lastAccessedTimestamp;
        private AtomicLong refCount = new AtomicLong(0);
        private long ttl;

        public TTLValue(long ts, long ttl) {
            this.lastAccessedTimestamp = new AtomicLong(ts);
            this.ttl = ttl;
        }

        public AtomicLong getLastAccessedTimestamp() {
            return lastAccessedTimestamp;
        }

        public void setLastAccessedTimestamp(AtomicLong lastAccessedTimestamp) {
            this.lastAccessedTimestamp = lastAccessedTimestamp;
        }

        public AtomicLong getRefCount() {
            return refCount;
        }

        public void setRefCount(AtomicLong refCount) {
            this.refCount = refCount;
        }

        public long getTtl() {
            return ttl;
        }

        public void setTtl(long ttl) {
            this.ttl = ttl;
        }

    }

    /**
     * A lazy mark and sweep,
     * a separate thread can also do this.
     */
    private Collection<V> lazyWillToDB() {

        Collection<V> values = null;
        keysToH2Db.clear();

        Set<K> keys = ttlMap.keySet();
        for(K key: keys) {
            TTLValue ttl = ttlMap.get(key);
            if ((System.currentTimeMillis()-
                    ttl.getLastAccessedTimestamp().get()) > ttl.getTtl()) {
                keysToH2Db.add(key);
            }
        }

        if (keysToH2Db.size() > 0) {
            values = new HashSet<V>();
            for(K key : keysToH2Db) {
                V v = lruMap.remove(key);
                values.add(v);
                lruMap.remove(key);
            }
        }

        return values;
    }

    @Override
    public Collection<V> getValues() {
        try {
            rLock.lock();
            Collection<V> col = new ArrayList<V>();
            for(V v : lruMap.values()) {
                col.add(v);
            }
            return col;
        } finally {
            rLock.unlock();
        }
    }
}
