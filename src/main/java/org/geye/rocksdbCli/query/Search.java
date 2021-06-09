package org.geye.rocksdbCli.query;

import com.alibaba.fastjson.JSONObject;
import org.geye.rocksdbCli.bean.QueryParams;
import org.geye.rocksdbCli.httpServer.utils.Configs;
import org.geye.rocksdbCli.httpServer.utils.utils;
import org.geye.rocksdbCli.query.futures.SearchSubSessionTask;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;

public class Search extends Query {

    private final TreeMap<Long, JSONObject> treeMap = new TreeMap<>();
    private final List<JSONObject> dataResult = new ArrayList<>();

    public Search(QueryParams queryParams) {
        params = queryParams;
    }

    public Search(long startTs, long endTs) {
        new Search(new QueryParams(startTs, endTs));
    }

    public Search doQuery() {
        System.out.println(params.toString());

        HashSet<String> calBucketSet = new HashSet<>(this.getBucketList());
        HashSet<String> allBucketSet = new HashSet<>(utils.getAllBucketList());

        allBucketSet.retainAll(calBucketSet);

        List<String> bucketList = new ArrayList<>(allBucketSet);
        bucketList.sort(Collections.reverseOrder());

        ExecutorService executorService = Executors.newFixedThreadPool(12);

        long t1 = System.currentTimeMillis();

        List<Future<List<JSONObject>>> futureList = new ArrayList<>();
        for (String bucket: bucketList) {
            File bucketDir = new File(Configs.BITMAP_HOME + "/" + bucket);
            if (!bucketDir.exists()) {
                continue;
            }

            List<String> dbPathList = this.getIndexDdPathList(bucket);
            if (dbPathList == null) continue;

            SearchSubSessionTask searchTask = new SearchSubSessionTask(bucket, params);
            Future<List<JSONObject>> future = executorService.submit(searchTask);

            futureList.add(future);
        }

        for (Future<List<JSONObject>> future: futureList) {
            try {
                List<JSONObject> tmpJsonArr = future.get();
                if (tmpJsonArr.size() <= 0) continue;
                for (JSONObject json: tmpJsonArr) {
                    this.treeMap.put(json.getLong("lastPacket") * -1, json);
                }

            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        executorService.shutdownNow();

        long t2 = System.currentTimeMillis();
        System.out.println("获取结果总共耗时（秒）: " + (float) (t2 - t1) / 1000);

        return this;
    }

    public List<JSONObject> result() {
        int hit = 0;
        for (Long k: treeMap.keySet()) {
            this.dataResult.add(treeMap.get(k));
            hit++;

            if (hit >= params.getLimit()) {
                break;
            }
        }
        return this.dataResult;
    }
}
