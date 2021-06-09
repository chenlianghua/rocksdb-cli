package org.geye.rocksdbCli.httpServer.service;

import org.geye.rocksdbCli.bean.RocksdbWithCF;
import org.geye.rocksdbCli.httpServer.cache.LRUCache;
import org.geye.rocksdbCli.httpServer.utils.Configs;
import org.geye.rocksdbCli.httpServer.utils.utils;
import org.geye.rocksdbCli.query.Query;
import org.rocksdb.*;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.*;

// @Component
public class SessionDbCacheInitService {

    protected static int CACHE_SIZE = 3 * 24 * 10;
    protected volatile static LRUCache indexCache;

    private SessionDbCacheInitService() {};

    // @PostConstruct
    public static LRUCache getCache() {
        if (indexCache == null) {
            synchronized (SessionDbCacheInitService.class) {
                if (indexCache == null) {
                    indexCache = new LRUCache(CACHE_SIZE);
                    initCache();
                }
            }
        }
        System.out.println("load session database cache...");

        return indexCache;
    }

    public static List<String> getAllBucketList() {

        File bucketDir = new File(Configs.INDEX_HOME);
        List<String> bucketList = Arrays.asList(Objects.requireNonNull(bucketDir.list()));

        bucketList.sort(Collections.reverseOrder());
        return bucketList;
    }

    public static void initCache() {

        String currentBucket = utils.getBucket(new Date().getTime());
        List<String> bucketList = getAllBucketList();

        Query query = new Query();

        int dbCnt = 0;
        for (String bucket: bucketList) {
            // if (bucket.equals(currentBucket)) continue;
            List<String> dbPathList = query.getSessionDbPathList(bucket);

            for (String dbPath: dbPathList) {

                if (dbCnt >= CACHE_SIZE) return;

                try {
                    RocksdbWithCF rocksdbWithCF = getDefaultDb(dbPath);

                    System.out.println("cache db: " + dbPath);

                    indexCache.put(dbPath, rocksdbWithCF);

                    dbCnt++;
                } catch (RocksDBException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static RocksdbWithCF getDefaultDb(String dbPath) throws RocksDBException {
        Options options = new Options();
        options.setCreateIfMissing(true);

        RocksDB rocksDB = RocksDB.openReadOnly(options, dbPath);

        return new RocksdbWithCF(rocksDB, rocksDB.getDefaultColumnFamily());
    }
}
