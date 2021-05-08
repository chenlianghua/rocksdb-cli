package org.geye.rocksdbCli.query;

import org.geye.rocksdbCli.bean.DocNode;
import org.geye.rocksdbCli.query.futures.SearchFuture;
import org.rocksdb.RocksDBException;

import java.util.*;
import java.util.concurrent.*;

public class Search extends Query {

    private String indexType;

    private final TreeMap<String, DocNode> treeMap = new TreeMap<>();
    private final List<DocNode> res = new ArrayList<>();

    public Search(long startTs, long endTs, String indexType, String target, int limit) {
        params.setStartTs(startTs);
        params.setEndTs(endTs);
        params.setTarget(formatSearchTarget(indexType, target));
        params.setLimit(limit);

        this.indexType = indexType;
    }

    public Search(long startTs, long endTs, String indexType, String target) {
        new Search(startTs, endTs, indexType, target, 10);
    }

    public Search doQuery() {

        List<String> bucketList = this.getBucketList();

        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(0,
                8,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());

        for (String bucket: bucketList) {
            List<String> dbPathList = this.getIndexDdPathList(bucket);
            if (dbPathList == null) continue;

            List<SearchFuture> futures = new ArrayList<>();

            for (String dbPath: dbPathList) {
                try {
                    SearchFuture searchFuture = new SearchFuture(dbPath, indexType, params);
                    threadPoolExecutor.submit(new Thread(searchFuture));

                    futures.add(searchFuture);
                } catch (RocksDBException e) {
                    e.printStackTrace();
                }
            }

            for (SearchFuture f: futures) {
                try {
                    List<DocNode> dataSet = f.get();
                    for (DocNode node: dataSet) {
                        treeMap.put(node.getK(), node);
                    }
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                    System.exit(0);
                }
            }

            if (treeMap.size() >= params.getLimit()) break;
        }

        threadPoolExecutor.shutdownNow();

        return this;
    }

    public List<DocNode> result() {
        for (String k: treeMap.keySet()) {
            this.res.add(treeMap.get(k));

            if (this.res.size() >= params.getLimit()) break;
        }
        return this.res;
    }
}
