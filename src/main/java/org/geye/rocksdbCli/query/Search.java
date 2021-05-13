package org.geye.rocksdbCli.query;

import org.geye.rocksdbCli.bean.DocNode;
import org.geye.rocksdbCli.query.futures.SearchTask;
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

        System.out.println(params.toString());

        List<String> bucketList = this.getBucketList();

        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(0,
                10,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());

        long t1 = System.currentTimeMillis();
        for (String bucket: bucketList) {
            List<String> dbPathList = this.getIndexDdPathList(bucket);
            if (dbPathList == null) continue;

            List<FutureTask<List<DocNode>>> futureTaskList = new ArrayList<>();
            for (String dbPath: dbPathList) {
                try {
                    SearchTask searchTask = new SearchTask(dbPath, indexType, params);
                    FutureTask<List<DocNode>> futureTask = new FutureTask<>(searchTask);
                    futureTask.run();

                    futureTaskList.add(futureTask);
                } catch (RocksDBException e) {
                    e.printStackTrace();
                }
            }

            for (FutureTask<List<DocNode>> f: futureTaskList) {
                try {
                    List<DocNode> dataSet = f.get();
                    for (DocNode node: dataSet) {
                        treeMap.put(node.k, node);
                    }
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                    System.exit(0);
                }
            }

            if (treeMap.size() >= params.getLimit()) break;
        }

        long t2 = System.currentTimeMillis();
        System.out.println("获取结果总共耗时（秒）: " + (float) (t2 - t1) / 1000);

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
