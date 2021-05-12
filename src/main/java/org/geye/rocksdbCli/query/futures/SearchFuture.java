package org.geye.rocksdbCli.query.futures;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.geye.rocksdbCli.bean.DocNode;
import org.geye.rocksdbCli.bean.QueryParams;
import org.geye.rocksdbCli.bean.RocksdbWithCF;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SearchFuture extends QueryFuture<List<DocNode>> {

    private List<DocNode> data = new ArrayList<>();

    private final String dbPath;
    private String indexType = new String(RocksDB.DEFAULT_COLUMN_FAMILY);
    private final QueryParams params;
    private final String[] indexBodyFields = {"firstPacket", "lastPacket", "srcIp", "dstIp", "protocol", "srcPort", "dstPort", "srcMac", "dstMac"};


    public SearchFuture(String dbPath, String indexType, QueryParams params) throws RocksDBException {
        this.dbPath = dbPath;
        this.indexType = indexType;
        this.params = params;
    }

    public List<DocNode> readSessionDb(List<byte[]> ids) {
        List<DocNode> sessions = new ArrayList<>();
        long t1 = System.currentTimeMillis();

        try {
            String sessionDbPath = this.dbPath.replace("index", "sessions");

            // TODO: 这一步最消耗时间，最好的办法是第一次把所有的库都加载到缓存里面
            RocksdbWithCF sessionDb  = this.getDefaultDb(sessionDbPath);


            List<byte[]> sessionList = sessionDb.db.multiGetAsList(ids);

            for (int i=0; i<sessionList.size(); i++) {

                byte[] keyBytes = ids.get(i);
                byte[] valBytes = sessionList.get(i);

                String key = new String(keyBytes);
                String val = new String(valBytes);

                DocNode docNode = new DocNode(key, val);
                sessions.add(docNode);
            }

            long t2 = System.currentTimeMillis();

            System.out.println("(" + Thread.currentThread().getName() + ")采用multiGet方式取值: " + (float) (t2 - t1) / 1000);

        } catch (RocksDBException e) {
            e.printStackTrace();
        }

        return sessions;
    }

    public JSONObject indexBodyToJSON(String raw) {
        JSONObject jsonObject = new JSONObject();

        String[] values = raw.split(",");

        for (int i=0; i<indexBodyFields.length; i++) {
            String field = indexBodyFields[i];

            if (field.equals("protocol") || field.equals("srcMac") || field.equals("dstMac")) {
                String[] rowSubVal = values[i].split("\\^");
                JSONArray jsonArray = new JSONArray();
                jsonArray.addAll(Arrays.asList(rowSubVal));

                jsonObject.put(field, jsonArray);
            } else {
                jsonObject.put(field, values[i]);
            }
        }

        return jsonObject;
    }

    public void readSingleDb(String dbPath, String indexType, String target, long startTs, long endTs, int limit) {
        try {
            String prefixFilter = String.format("%s%s", indexType, target);

            RocksdbWithCF rocksObj = getDb(dbPath, indexType);
            RocksIterator iterator= getIterator(rocksObj, prefixFilter, startTs, endTs);

            int cnt = 0;
            for (iterator.seek(prefixFilter.getBytes(StandardCharsets.UTF_8)); iterator.isValid(); iterator.next()) {
                iterator.status();

                try {
                    assert (iterator.key() != null);
                    assert (iterator.value() != null);

                    byte[] keyBytes = iterator.key();
                    byte[] valBytes = iterator.value();

                    String key = new String(keyBytes);
                    String value = new String(valBytes);

                    DocNode docNode = new DocNode(key, this.indexBodyToJSON(value).toString());
                    this.data.add(docNode);

                    cnt++;

                    if (cnt >= limit || !key.startsWith(prefixFilter)) {
                        break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            iterator.close();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        this.running = true;
        super.run();

        String target = this.params.getTarget();
        long startTs = this.params.getStartTs();
        long endTs = this.params.getEndTs();
        int limit = this.params.getLimit();

        this.readSingleDb(dbPath, indexType, target, startTs, endTs, limit);

        this.running = false;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return !this.running;
    }

    @Override
    public List<DocNode> get() throws InterruptedException, ExecutionException {
        while (this.running) {
            Thread.sleep(50);
        }
        return this.data;
    }

    @Override
    public List<DocNode> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return this.data;
    }

    public String getDbPath() { return this.dbPath; }
}
