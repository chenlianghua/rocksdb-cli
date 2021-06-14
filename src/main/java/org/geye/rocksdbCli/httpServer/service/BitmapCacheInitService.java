package org.geye.rocksdbCli.httpServer.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.geye.rocksdbCli.bean.RocksdbWithCF;
import org.geye.rocksdbCli.httpServer.cache.LRUCache;
import org.geye.rocksdbCli.httpServer.utils.Configs;
import org.geye.rocksdbCli.httpServer.utils.utils;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;

// @Component
public class BitmapCacheInitService {

    protected static int CACHE_SIZE = 7 * 24 * 5;
    protected volatile static Cache<String, RocksdbWithCF> indexCache;

    private BitmapCacheInitService() {};

    // @PostConstruct
    public static Cache<String, RocksdbWithCF> getCache() {

        if (indexCache == null) {
            synchronized (BitmapCacheInitService.class) {
                if (indexCache == null) {
                    indexCache = Caffeine.newBuilder()
                            .maximumSize(CACHE_SIZE)
                            .initialCapacity(CACHE_SIZE)
                            .removalListener(
                                    (String key, Object value, RemovalCause cause) -> {
                                        System.out.println(String.format("bitmap index cache key: %s was removed (%s)%n", key, cause));
                                    })
                            .build();;
                    initCache();
                }
            }
            System.out.println("load bitmap index cache...");
        }

        return indexCache;
    }

    public static RocksdbWithCF getDb(String dbPath, String indexType) throws RocksDBException {
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

        return new RocksdbWithCF(db, cfHandles.get(cfHandles.size() - 1));
    }

    private static void initCache() {

        String currentBucket = utils.getBucket(new Date().getTime());
        List<String> bucketList = utils.getAllBucketList();

        int dbCnt = 0;
        for (String bucket: bucketList) {
            if (bucket.equals(currentBucket)) continue;
            String dbPath = Configs.BITMAP_HOME + "/" + bucket;

            if (dbCnt >= CACHE_SIZE) return;

            for (String indexType: Configs.ALL_INDEX_TYPE) {
                try {

                    String cacheKey = utils.buildKey(dbPath, indexType);

                    RocksdbWithCF rocksdbWithCF = getDb(dbPath, indexType);
                    indexCache.put(cacheKey, rocksdbWithCF);

                    dbCnt++;
                } catch (RocksDBException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
