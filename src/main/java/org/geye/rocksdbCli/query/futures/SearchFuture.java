package org.geye.rocksdbCli.query.futures;

import org.geye.rocksdbCli.bean.DocNode;
import org.geye.rocksdbCli.bean.IndexNode;
import org.geye.rocksdbCli.bean.QueryParams;
import org.geye.rocksdbCli.bean.RocksdbWithCF;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SearchFuture extends QueryFuture<List<DocNode>> {

    private List<DocNode> data = new ArrayList<>();

    private final String dbPath;
    private String indexType = new String(RocksDB.DEFAULT_COLUMN_FAMILY);
    private final QueryParams params;

    public SearchFuture(String dbPath, String indexType, QueryParams params) {
        this.dbPath = dbPath;
        this.indexType = indexType;
        this.params = params;
    }

    public SearchFuture(String dbPath, QueryParams params) {
        this.dbPath = dbPath;
        this.params = params;
    }

    public List<DocNode> readSessionDb(List<byte[]> ids) {
        List<DocNode> sessions = new ArrayList<>();
        long t1 = System.currentTimeMillis();

        String sessionDbPath = this.dbPath.replace("index", "sessions");
        try {
            RocksdbWithCF rocksdb = getDefaultDb(sessionDbPath);
            List<byte[]> sessionList = rocksdb.db.multiGetAsList(ids);

            for (int i=0; i<sessionList.size(); i++) {

                byte[] keyBytes = ids.get(i);
                byte[] valBytes = sessionList.get(i);

                String key = new String(keyBytes);
                String val = new String(valBytes);

                DocNode docNode = new DocNode(key, val);
                sessions.add(docNode);
            }

            rocksdb.close();

        } catch (RocksDBException e) {
            e.printStackTrace();
        }

        long t2 = System.currentTimeMillis();

        System.out.println(Thread.currentThread().getName() + ": " + (float) (t2 - t1) / 1000);
        return sessions;
    }

    public void readSingleDb(String dbPath, String indexType, String target, long startTs, long endTs, int limit) {
        try {
            List<byte[]> idList = new ArrayList<>();
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
                    // String value = new String(valBytes);

                    // IndexNode indexNode = new IndexNode(key, value, this.dbPath.replace("index", "sessions"));
                    // this.data.add(indexNode);

                    idList.add(valBytes);

                    cnt++;

                    if (cnt >= limit || !key.startsWith(prefixFilter)) {
                        break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (idList.size() > 0) {
                List<DocNode> docNodes = this.readSessionDb(idList);
                this.data = docNodes;
            }

            iterator.close();
            rocksObj.close();
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
        return this.data;
    }

    @Override
    public List<DocNode> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return this.data;
    }
}
