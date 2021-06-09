package org.geye.rocksdbCli.httpServer.service;

import org.geye.rocksdbCli.bean.RocksdbWithCF;
import org.geye.rocksdbCli.httpServer.cache.LRUCache;
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
public class IndexCacheInitService {

    protected static int CACHE_SIZE = 3 * 24 * 10;
    protected volatile static LRUCache indexCache;

    private IndexCacheInitService() {};

    @PostConstruct
    public static LRUCache getCache() {

        if (indexCache == null) {
            synchronized (IndexCacheInitService.class) {
                if (indexCache == null) {
                    indexCache = new LRUCache(CACHE_SIZE);
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

                for (String indexType: Configs.ALL_INDEX_TYPE) {
                    try {

                        RocksdbWithCF rocksdbWithCF = getDb(dbPath);
                        indexCache.put(dbPath, indexType, rocksdbWithCF);

                        dbCnt++;
                    } catch (RocksDBException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
