package org.geye.rocksdbCli.bean;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

public class RocksdbWithCF {
    public RocksDB db;
    public ColumnFamilyHandle cfHandler;

    public RocksdbWithCF(RocksDB db, ColumnFamilyHandle handler) {
        this.db = db;
        this.cfHandler = handler;
    }

    public void close() {
        if (this.cfHandler != null) this.cfHandler.close();

        if (this.db != null) this.db.close();
    }
}