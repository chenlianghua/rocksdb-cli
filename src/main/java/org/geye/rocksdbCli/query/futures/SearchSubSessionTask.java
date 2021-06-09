package org.geye.rocksdbCli.query.futures;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.geye.rocksdbCli.bean.QueryParams;
import org.geye.rocksdbCli.bean.RocksdbWithCF;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.*;

public class SearchSubSessionTask extends QueryTask implements Callable<List<JSONObject>> {

    private final String bucket;
    private final QueryParams params;
    private final String[] indexBodyFields = {"firstPacket", "lastPacket", "srcIp", "dstIp", "protocol", "srcPort", "dstPort", "srcMac", "dstMac"};

    public SearchSubSessionTask(String bucket, QueryParams params) {
        this.bucket = bucket;
        this.params = params;
    }

    public Roaring64Bitmap readBitmapDb() {
        SearchBitmapTask searchBitmapTask = new SearchBitmapTask(bucket, params);
        Roaring64Bitmap bucketBitmap = new Roaring64Bitmap();
        try {
            bucketBitmap = searchBitmapTask.fetchBitmap();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return bucketBitmap;
    }

    public List<JSONObject> readSubSessionDb(long[] ids) throws RocksDBException {
        List<JSONObject> data = new ArrayList<>();

        List<String> subSessionList = this.getIndexDdPathList(bucket);

        List<byte[]> idBytes = new ArrayList<>();
        for (long id: ids) {
            String idStr = String.format("%13d", id);
            idBytes.add(idStr.getBytes(StandardCharsets.UTF_8));
        }

        List<Future<List<JSONObject>>> futureList = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(subSessionList.size());
        for (String subSessionDb: subSessionList) {
            Future<List<JSONObject>> future = executorService.submit(new Callable<List<JSONObject>>() {
                @Override
                public List<JSONObject> call() throws Exception {
                    List<JSONObject> tmpData = new ArrayList<>();

                    try {
                        RocksdbWithCF rocks = getSubSessionDb(subSessionDb);

                        List<byte[]> bytes = rocks.db.multiGetAsList(idBytes);
                        for (byte[] hitByte : bytes) {
                            if (hitByte == null) continue;

                            String rawHit = new String(hitByte);
                            JSONObject jsonDoc = indexBodyToJSON(rawHit);

                            tmpData.add(jsonDoc);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    return tmpData;
                }
            });

            futureList.add(future);
        }

        for (Future<List<JSONObject>> future: futureList) {
            try {
                List<JSONObject> tmpData = future.get();
                if (tmpData.size() <= 0) continue;

                data.addAll(tmpData);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        executorService.shutdownNow();

        return data;
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

    @Override
    public List<JSONObject> call() {
        List<JSONObject> jsonObjects = new ArrayList<>();

        Roaring64Bitmap roaring64Bitmap = this.readBitmapDb();

        if (!roaring64Bitmap.isEmpty()) {
            try {
                long[] subSids = roaring64Bitmap.reverseStream().limit(params.getLimit()).toArray();

                long t1 = System.currentTimeMillis();
                jsonObjects = this.readSubSessionDb(subSids);
                Collections.sort(jsonObjects, new Comparator<JSONObject>(){
                    public int compare(JSONObject arg0, JSONObject arg1) {
                        long ts1 = Long.parseLong(arg0.getString("lastPacket"));
                        long ts2 = Long.parseLong(arg1.getString("lastPacket"));

                        return (int) (ts1 - ts2);
                    }
                });

                long t2 = System.currentTimeMillis();
                float cost = (float) (t2 - t1) / 1000;

                System.out.println(bucket + ": fetch sub session cost: " + cost);
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        }

        this.running = false;

        return jsonObjects;
    }
}
