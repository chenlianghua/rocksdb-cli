package org.geye.rocksdbCli.query.futures;

import org.geye.rocksdbCli.bean.QueryParams;
import org.geye.rocksdbCli.bean.RocksdbWithCF;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class CountFuture extends QueryTask {

    double cnt = 0;

    private final String dbPath;
    private final QueryParams params;
    private String indexType = new String(RocksDB.DEFAULT_COLUMN_FAMILY);

    public CountFuture(String dbPath, String indexType, QueryParams params) {
        this.dbPath = dbPath;
        this.indexType = indexType;
        this.params = params;
    }

    public CountFuture(String dbPath, QueryParams params) {
        this.dbPath = dbPath;
        this.params = params;
    }

    @Override
    public void run() {
        this.running = true;
        super.run();
        this.cnt = this.count(dbPath, this.indexType);

        this.running = false;
    }

    public double count(String dbPath, String indexType) {
        double sum = 0;

        try {
            RocksdbWithCF rocksObj = getBitmapDb(dbPath, indexType);
            RocksIterator iterator= getIterator(rocksObj);

            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                sum += 1;
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

        System.out.println(dbPath + ": " + sum);
        return sum;
    }

}
