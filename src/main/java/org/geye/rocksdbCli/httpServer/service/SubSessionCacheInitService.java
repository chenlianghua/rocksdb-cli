package org.geye.rocksdbCli.httpServer.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.geye.rocksdbCli.bean.RocksdbWithCF;
import org.geye.rocksdbCli.httpServer.utils.Configs;
import org.geye.rocksdbCli.httpServer.utils.utils;
import org.geye.rocksdbCli.query.Query;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.*;

@Component
public class SubSessionCacheInitService {

    protected static int CACHE_SIZE = 7 * 24 * 10 * 5;
    protected volatile static Cache<String, RocksdbWithCF> indexCache;

    private SubSessionCacheInitService() {};

    @PostConstruct
    public static Cache<String, RocksdbWithCF> getCache() {

        if (indexCache == null) {
            synchronized (SubSessionCacheInitService.class) {
                if (indexCache == null) {
                    indexCache = Caffeine.newBuilder()
                            .maximumSize(CACHE_SIZE)
                            .initialCapacity(CACHE_SIZE)
                            .removalListener(
                                    (String key, Object value, RemovalCause cause) -> {
                                        System.out.println(String.format("sub session cache key: %s was removed (%s)%n", key, cause));
                                    })
                            .build();;
                    initCache();
                }
            }
            System.out.println("load session index cache...");
        }

        return indexCache;
    }

    public static List<String> getAllBucketList() {

        File bucketDir = new File(Configs.INDEX_HOME);
        List<String> bucketList = Arrays.asList(Objects.requireNonNull(bucketDir.list()));

        bucketList.sort(Collections.reverseOrder());
        return bucketList;
    }

    public static RocksdbWithCF getDb(String dbPath) throws RocksDBException {
        List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

        ColumnFamilyDescriptor defaultCfDescriptor = new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        cfDescriptors.add(defaultCfDescriptor);

        RocksDB db = RocksDB.openReadOnly(dbPath, cfDescriptors, cfHandles);

        return new RocksdbWithCF(db, cfHandles.get(cfHandles.size() - 1));
    }

    private static void initCache() {

        String currentBucket = utils.getBucket(new Date().getTime());
        List<String> bucketList = getAllBucketList();

        Query query = new Query();

        int dbCnt = 0;
        for (String bucket: bucketList) {
            // if (bucket.equals(currentBucket)) continue;
            List<String> indexPathList = query.getIndexDdPathList(bucket);

            for (String dbPath: indexPathList) {

                if (dbCnt >= CACHE_SIZE) return;

                try {

                    RocksdbWithCF rocksdbWithCF = getDb(dbPath);
                    String cacheKey = utils.buildKey(dbPath, new String(RocksDB.DEFAULT_COLUMN_FAMILY));
                    indexCache.put(cacheKey, rocksdbWithCF);

                    dbCnt++;
                } catch (RocksDBException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
