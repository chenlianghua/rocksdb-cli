package org.geye.rocksdbCli.query.futures;

import org.geye.rocksdbCli.bean.RocksdbWithCF;
import org.geye.rocksdbCli.httpServer.cache.LRUCache;
import org.geye.rocksdbCli.httpServer.service.IndexCacheInitService;
import org.geye.rocksdbCli.httpServer.service.SessionDbCacheInitService;
import org.geye.rocksdbCli.query.Query;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;


public class QueryTask extends Query {

    boolean running = true;
    boolean isCancelled = false;

    public void run() {
        if (this.isCancelled) return;

        this.running = true;
    }

    public RocksIterator getIterator(RocksdbWithCF rocksdb) {
        return rocksdb.db.newIterator(rocksdb.cfHandler, new ReadOptions());
    }

    public RocksdbWithCF getBitmapDb(String dbPath, String indexType) throws RocksDBException {

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
            String cfName = String.format("bitmapCF-%s", indexType);
            ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(cfName.getBytes(StandardCharsets.UTF_8));
            cfDescriptors.add(cfDescriptor);
        }

        RocksDB db = RocksDB.openReadOnly(dbPath, cfDescriptors, cfHandles);
        rocksdbWithCF = new RocksdbWithCF(db, cfHandles.get(cfHandles.size() - 1));

        cache.put(dbPath, indexType, rocksdbWithCF);

        return rocksdbWithCF;
    }

    public RocksdbWithCF getSubSessionDb(String dbPath) throws RocksDBException {
        RocksdbWithCF rocksdbWithCF;
        LRUCache cache = IndexCacheInitService.getCache();

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

    public RocksdbWithCF getSessionDb(String dbPath) throws RocksDBException {
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
