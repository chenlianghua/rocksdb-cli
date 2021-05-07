package org.geye.rocksdbCli;

import org.apache.commons.lang.time.FastDateFormat;
import org.geye.rocksdbCli.bean.QueryParams;
import org.geye.rocksdbCli.query.futures.CountFuture;
import org.geye.rocksdbCli.query.futures.SearchFuture;
import org.rocksdb.*;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;


public class RocksdbReader {
    static {
        RocksDB.loadLibrary();
    }

    private String dbHome = "/data0/rocksdb";
    private String indexHome = String.format("%s/index", dbHome);
    private String sessionHome = String.format("%s/sessions", dbHome);
    private FastDateFormat tsFormatter = FastDateFormat.getInstance("yMMdd'h'HH");

    public void doSearch(String action, long startTs, long endTs, String indexType, String target, int limit) {

        List<String> bucketList = new ArrayList<>();

        long tmpTs = startTs;
        while (tmpTs <= endTs) {
            String bucket = tsFormatter.format(tmpTs);
            bucketList.add(bucket);

            tmpTs += 3600 * 1000;
        }

        QueryParams queryParams = new QueryParams();
        queryParams.setTarget(target);
        queryParams.setStartTs(startTs);
        queryParams.setEndTs(endTs);
        queryParams.setLimit(limit);

        for (String bucket: bucketList) {
            double sum = 0;
            String bucketPath = this.indexHome + "/" + bucket;
            File bucketDir = new File(bucketPath);

            if (!bucketDir.exists()) continue;

            ThreadPoolExecutor tPool = new ThreadPoolExecutor(0, 8, 60L, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>());

            List<Future<?>> res = new ArrayList<>();

            for (String subBucket: Objects.requireNonNull(bucketDir.list())) {
                String dbPath = bucketPath + "/" + subBucket;
                Future<?> f;
                switch (action) {
                    case "count":
                        f = tPool.submit(new CountFuture(dbPath, queryParams));
                        break;
                    case "search":
                    default:
                        f = tPool.submit(new SearchFuture(dbPath, indexType, queryParams));
                        break;
                }
                res.add(f);
            }

            for (Future<?> f: res) {
                try {
                    f.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }

            if (action.equals("count")) System.out.println(bucketPath + ": " + sum);
        }
    }
}
