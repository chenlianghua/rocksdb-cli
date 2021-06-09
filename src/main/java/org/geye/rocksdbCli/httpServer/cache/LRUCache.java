package org.geye.rocksdbCli.httpServer.cache;

import org.geye.rocksdbCli.bean.RocksdbWithCF;
import org.rocksdb.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.geye.rocksdbCli.httpServer.utils.utils.buildKey;

public class LRUCache{

    private int MAX_SIZE = 3 * 24 * 8 * 6;
    private int size = 0;

    private CacheNode headNode;
    private CacheNode tailNode;

    private final String DEFAULT_CF = new String(RocksDB.DEFAULT_COLUMN_FAMILY);

    public ConcurrentHashMap<String, CacheNode> rocksdbMap = new ConcurrentHashMap<>();

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();

    private void initCache() {
        headNode = new CacheNode();
        tailNode = new CacheNode();

        headNode.next = tailNode;
        tailNode.prev = headNode;
    }

    public LRUCache() {
        this.initCache();
    }

    public LRUCache(int maxSize) {
        this.MAX_SIZE = maxSize;
        this.initCache();
    }

    public void addToHead(CacheNode cacheNode) {
        headNode.next = cacheNode;
        cacheNode.prev = headNode;
        cacheNode.next = tailNode;
        tailNode.prev = cacheNode;

        size++;
    }

    public CacheNode removeLast() {
        CacheNode lastNode = tailNode.prev;
        removeNode(lastNode);

        return lastNode;
    }

    public void removeNode(CacheNode cacheNode) {
        CacheNode prevNode = cacheNode.prev;
        CacheNode nextNode = cacheNode.next;

        prevNode.next = nextNode;
        nextNode.prev = prevNode;

        size--;
    }

    public void moveToHead(CacheNode cacheNode) {
        removeNode(cacheNode);
        addToHead(cacheNode);
    }

    public RocksdbWithCF get(String dbPath) {
        return this.get(dbPath, this.DEFAULT_CF);
    }

    public RocksdbWithCF get(String dbPath, String indexType) {
        String key = buildKey(dbPath, indexType);

        CacheNode cacheNode;
        RocksdbWithCF obj = null;

        this.readLock.lock();
        if (rocksdbMap.get(key) != null) {

            cacheNode = rocksdbMap.get(key);
            moveToHead(cacheNode);
            obj = cacheNode.val;
        }
        this.readLock.unlock();

        return obj;
    }

    public void put(String dbPath, String indexType, RocksdbWithCF obj) {
        this.writeLock.lock();
        String key = buildKey(dbPath, indexType);

        CacheNode existingNode = this.rocksdbMap.get(key);
        if (existingNode != null) {

            this.removeNode(existingNode);
        }
        CacheNode cacheNode = new CacheNode(key, obj);

        addToHead(cacheNode);
        this.rocksdbMap.put(key, cacheNode);

        while (size > MAX_SIZE) {
            CacheNode lastNode = this.removeLast();
            this.rocksdbMap.remove(lastNode.key);
        }

        this.writeLock.unlock();
    }

    public void put(String dbPath, RocksdbWithCF obj) {
        this.put(dbPath, this.DEFAULT_CF, obj);
    }

    public void remove(String dbPath, String indexType) {
        String key = buildKey(dbPath, indexType);

        this.writeLock.lock();

        if (this.rocksdbMap.containsKey(key)) {
            CacheNode nodeToRemove = this.rocksdbMap.get(key);
            this.removeNode(nodeToRemove);
            this.rocksdbMap.remove(key);
        }

        this.writeLock.unlock();
    }

    public void remove(String dbPath) {
        this.remove(dbPath, this.DEFAULT_CF);
    }

    public void cleanAllCache() {
        this.writeLock.lock();

        this.rocksdbMap = new ConcurrentHashMap<>();
        this.initCache();
        size = 0;

        this.writeLock.unlock();
    }
}


class CacheNode{
    public String key;
    public RocksdbWithCF val;

    public CacheNode prev;
    public CacheNode next;

    public CacheNode () {}

    public CacheNode(String key, RocksdbWithCF val) {
        this.key = key;
        this.val = val;
    }

    public void close() {
        if (this.val != null) this.val.close();
    }
}
