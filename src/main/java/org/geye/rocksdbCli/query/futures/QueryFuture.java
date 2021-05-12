package org.geye.rocksdbCli.query.futures;

import org.geye.rocksdbCli.bean.RocksdbWithCF;
import org.geye.rocksdbCli.httpServer.cache.LRUCache;
import org.geye.rocksdbCli.httpServer.service.IndexCacheInitService;
import org.geye.rocksdbCli.httpServer.service.SessionDbCacheInitService;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public abstract class QueryFuture<V> implements RunnableFuture<V> {

    boolean running = true;

    @Override
    public void run() {

    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return !this.running;
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        return null;
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return null;
    }

    public RocksIterator getIterator(RocksdbWithCF rocksdb) {
        return rocksdb.db.newIterator(rocksdb.cfHandler, new ReadOptions());
    }

    public RocksIterator getIterator(RocksdbWithCF rocksObj, String prefixFilter, long startTs, long endTs) {
        ReadOptions readOptions = new ReadOptions();

        String lowerBound = String.format("%s:%s:%s", prefixFilter, startTs, "00000000000000");
        String upperBound = String.format("%s:%s:%s", prefixFilter, endTs, System.nanoTime());

        readOptions.setIterateLowerBound(new Slice(lowerBound.getBytes(StandardCharsets.UTF_8)));
        readOptions.setIterateUpperBound(new Slice(upperBound.getBytes(StandardCharsets.UTF_8)));

        return rocksObj.db.newIterator(rocksObj.cfHandler, readOptions);
    }

    public RocksdbWithCF getDb(String dbPath, String indexType) throws RocksDBException {

        LRUCache cache = IndexCacheInitService.getCache();

        RocksdbWithCF rocksdbWithCF = cache.get(dbPath, indexType);
        if (rocksdbWithCF != null) {
            return rocksdbWithCF;
        }

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
        rocksdbWithCF = new RocksdbWithCF(db, cfHandles.get(cfHandles.size() - 1));

        cache.put(dbPath, indexType, rocksdbWithCF);

        return rocksdbWithCF;
    }

    public RocksdbWithCF getDefaultDb(String dbPath) throws RocksDBException {
        RocksdbWithCF rocksdbWithCF;
        LRUCache cache = SessionDbCacheInitService.getCache();

        rocksdbWithCF = cache.get(dbPath);

        if (rocksdbWithCF != null) {
            return rocksdbWithCF;
        }

        Options options = new Options();
        options.setCreateIfMissing(true);

        RocksDB rocksDB = RocksDB.openReadOnly(options, dbPath);

        rocksdbWithCF = new RocksdbWithCF(rocksDB, rocksDB.getDefaultColumnFamily());
        cache.put(dbPath, rocksdbWithCF);

        return rocksdbWithCF;
    }
}
