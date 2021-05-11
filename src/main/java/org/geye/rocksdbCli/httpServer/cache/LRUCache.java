package org.geye.rocksdbCli.httpServer.cache;

import org.geye.rocksdbCli.bean.RocksdbWithCF;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class LRUCache{

    private int MAX_SIZE = 3 * 24 * 8 * 6;
    private int size = 0;

    private CacheNode headNode;
    private CacheNode tailNode;

    private final String DEFAULT_CF = new String(RocksDB.DEFAULT_COLUMN_FAMILY);

    private ConcurrentHashMap<String, CacheNode> rocksdbMap = new ConcurrentHashMap<>();

    private final ReentrantLock lock = new ReentrantLock();

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

    public String buildKey(String dbPath, String indexType) {
        return String.format("%s:%s", dbPath, indexType);
    }

    public RocksdbWithCF getDb(String dbPath, String indexType) throws RocksDBException {
        List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

        ColumnFamilyDescriptor defaultCfDescriptor = new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        cfDescriptors.add(defaultCfDescriptor);

        if (!indexType.equals(new String(RocksDB.DEFAULT_COLUMN_FAMILY))) {
            String cfName = String.format("flowIndexCF-%s", indexType);
            ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(cfName.getBytes(StandardCharsets.UTF_8));
            cfDescriptors.add(cfDescriptor);
        }

        RocksDB db = RocksDB.openReadOnly(dbPath, cfDescriptors, cfHandles);

        return new RocksdbWithCF(db, cfHandles.get(cfHandles.size() - 1));
    }

    public RocksdbWithCF getDefaultDb(String dbPath) throws RocksDBException {
        Options options = new Options();
        options.setCreateIfMissing(true);

        RocksDB rocksDB = RocksDB.openReadOnly(options, dbPath);

        return new RocksdbWithCF(rocksDB, rocksDB.getDefaultColumnFamily());
    }

    public RocksdbWithCF get(String dbPath) {
        return this.get(dbPath, this.DEFAULT_CF);
    }

    public RocksdbWithCF get(String dbPath, String indexType) {
        this.lock.lock();
        String key = this.buildKey(dbPath, indexType);

        CacheNode cacheNode;
        RocksdbWithCF obj = null;

        if (rocksdbMap.get(key) != null) {
            cacheNode = rocksdbMap.get(key);
            moveToHead(cacheNode);
            obj = cacheNode.val;
        }

        this.lock.unlock();
        return obj;
    }

    public void put(String dbPath) {
        this.put(dbPath, this.DEFAULT_CF);
    }

    public void put(String dbPath, String indexType) {
        this.lock.lock();
        RocksdbWithCF rocksdb = this.get(dbPath, indexType);
        String key = this.buildKey(dbPath, indexType);

        if (rocksdb != null) {
            CacheNode existingNode = this.rocksdbMap.get(key);

            this.removeNode(existingNode);
        }

        try {
            rocksdb = indexType.equals(DEFAULT_CF) ? this.getDefaultDb(dbPath) : this.getDb(dbPath, indexType);
            CacheNode cacheNode = new CacheNode(key, rocksdb);

            addToHead(cacheNode);
            this.rocksdbMap.put(key, cacheNode);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

        while (size > MAX_SIZE) {
            CacheNode lastNode = this.removeLast();
            this.rocksdbMap.remove(lastNode.key);
        }

        this.lock.unlock();
    }

    public void remove(String dbPath, String indexType) {
        String key = this.buildKey(dbPath, indexType);

        this.lock.lock();

        if (this.rocksdbMap.containsKey(key)) {
            CacheNode nodeToRemove = this.rocksdbMap.get(key);
            this.removeNode(nodeToRemove);
            this.rocksdbMap.remove(key);
        }

        this.lock.unlock();
    }

    public void cleanAllCache() {
        this.lock.lock();

        this.rocksdbMap = new ConcurrentHashMap<>();
        this.initCache();
        size = 0;

        this.lock.unlock();
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
}
