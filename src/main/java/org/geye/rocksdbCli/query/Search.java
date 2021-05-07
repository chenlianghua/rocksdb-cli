package org.geye.rocksdbCli.query;

import org.geye.rocksdbCli.bean.DocNode;
import org.geye.rocksdbCli.bean.IndexNode;
import org.geye.rocksdbCli.query.futures.SearchFuture;

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
        int l1LoopCnt = 0;
        int loopStep = 2;

        List<String> bucketList = this.getBucketList();

        for (String bucket: bucketList) {
            l1LoopCnt++;

            List<String> dbPathList = this.getDdPathList(bucket);
            if (dbPathList == null) continue;

            List<SearchFuture> futures = new ArrayList<>();

            for (String dbPath: dbPathList) {
                SearchFuture searchFuture = new SearchFuture(dbPath, indexType, params);

                futures.add(searchFuture);
                searchFuture.run();
            }

            // 一次查询2个库
            if (l1LoopCnt % loopStep != 0) continue;

            for (SearchFuture f: futures) {
                try {
                    List<DocNode> dataSet = f.get();
                    if (dataSet == null) continue;
                    for (DocNode node: dataSet) {
                        treeMap.put(node.getK(), node);
                    }
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }

            if (treeMap.size() > params.getLimit()) break;
        }

        return this;
    }

    public List<DocNode> result() {
        for (String k: treeMap.keySet()) {
            this.res.add(treeMap.get(k));

            if (this.res.size() > params.getLimit()) break;
        }
        return this.res;
    }
}
