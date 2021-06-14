package org.geye.rocksdbCli.httpServer.cache;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RocksdbHandlerCache {

    private int MAX_SIZE = 3 * 24 * 8 * 6;
    private int size = 0;

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();

    public RocksdbHandlerCache(int maxSize) {
        this.MAX_SIZE = maxSize;
    }


}
