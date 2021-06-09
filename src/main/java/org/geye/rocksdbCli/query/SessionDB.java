package org.geye.rocksdbCli.query;

import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SessionDB {

    static {
        RocksDB.loadLibrary();
    }

    protected static final Logger logger = LoggerFactory.getLogger(SessionDB.class);
    protected static final String CONFIG_FILE = "job-es-tfs.properties";

    protected String dbPath;
    protected DBOptions dbOpts;
    protected Options options;

    protected HashMap<String, ColumnFamilyHandle> cfHandlers = new HashMap<>();

    public RocksDB db;

    public SessionDB(String dbPath) throws IOException, RocksDBException {
        this.dbPath = dbPath;
        this.db = getDBObject();
    };

    public Options getOptions() {
        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        Options options = new Options();

        options.setCreateIfMissing(true);

        // writeBufferSize is the size of memtable of column family, default: 64MB
        options.setWriteBufferSize(256 * SizeUnit.MB);

        // MaxWriteBufferNumber is the max number of memtable, default: 2
        options.setMaxWriteBufferNumber(10);

        // trigger of memtable to merge into level0 files, default: 1
        options.setMinWriteBufferNumberToMerge(2);

        // trigger of level0 files to merge into level1 files, default: 4
        options.setLevel0FileNumCompactionTrigger(2);

        // max number of memtable flush threads
        options.setMaxBackgroundJobs(5);

        // max number of compaction jobs
        options.setMaxSubcompactions(5);

        options.setMaxBytesForLevelBase(options.writeBufferSize() * options.minWriteBufferNumberToMerge() * options.level0FileNumCompactionTrigger());
        options.setTargetFileSizeBase(options.maxBytesForLevelBase() / 10);

        options.setMemtablePrefixBloomSizeRatio(0.85);

        return options.prepareForBulkLoad();
    }

    public ColumnFamilyOptions getIndexCFOpts() {
        ColumnFamilyOptions cfOpts = new ColumnFamilyOptions();
        BlockBasedTableConfig tableConfig = this.getBlockBasedTableCfg();

        // tableConfig.setBlockCache(new LRUCache(16 * SizeUnit.MB));
        // tableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
        // tableConfig.setPinTopLevelIndexAndFilter(true);
        // tableConfig.setCacheIndexAndFilterBlocks(true);

        cfOpts.setTableFormatConfig(tableConfig);

        // writeBufferSize is the size of memtable of column family, default: 64MB
        cfOpts.setWriteBufferSize(64 * SizeUnit.MB);

        // MaxWriteBufferNumber is the max number of memtable, default: 2
        cfOpts.setMaxWriteBufferNumber(10);

        // trigger of memtable to merge into level0 files, default: 1
        cfOpts.setMinWriteBufferNumberToMerge(2);

        // trigger of level0 files to merge into level1 files, default: 4
        cfOpts.setLevel0FileNumCompactionTrigger(2);

        // set compression to LZ4 to speed up point search
        cfOpts.setCompressionType(CompressionType.LZ4_COMPRESSION);

        cfOpts.setMaxBytesForLevelBase(cfOpts.writeBufferSize() * cfOpts.minWriteBufferNumberToMerge() * cfOpts.level0FileNumCompactionTrigger());
        cfOpts.setTargetFileSizeBase(cfOpts.maxBytesForLevelBase() / 10);

        cfOpts.setMemtablePrefixBloomSizeRatio(0.85);

        return cfOpts;
    }

    public BlockBasedTableConfig getBlockBasedTableCfg() {

        int bloomFilterSize = 10;

        // block based table configuration =============================
        BlockBasedTableConfig blockBasedTableCfg = new BlockBasedTableConfig();

        // improve the search efficient of Get()
        blockBasedTableCfg.setDataBlockIndexType(DataBlockIndexType.kDataBlockBinaryAndHash);
        blockBasedTableCfg.setDataBlockHashTableUtilRatio(0.85);

        // enable block cache
        // blockBasedTableCfg.setBlockCache(new LRUCache(500 * SizeUnit.MB));

        // enable bloom filter
        blockBasedTableCfg.setFilterPolicy(new BloomFilter(bloomFilterSize, false));
        blockBasedTableCfg.setIndexType(IndexType.kTwoLevelIndexSearch);
        blockBasedTableCfg.setPartitionFilters(true);
        // blockBasedTableCfg.setCacheIndexAndFilterBlocksWithHighPriority(true);
        // blockBasedTableCfg.setPinTopLevelIndexAndFilter(true);
        // blockBasedTableCfg.setCacheIndexAndFilterBlocks(true);
        blockBasedTableCfg.setOptimizeFiltersForMemory(true);
        return blockBasedTableCfg;
    }


    public RocksDB getDBObject() throws RocksDBException, IOException {

        if (this.options == null) this.options = getOptions();

        BlockBasedTableConfig blockBasedTableCfg = getBlockBasedTableCfg();

        options.setTableFormatConfig(blockBasedTableCfg);

        // 设置每层压缩类型
        // List<CompressionType> levelCompressList = new ArrayList<>();
        //
        // levelCompressList.add(CompressionType.LZ4_COMPRESSION);
        // levelCompressList.add(CompressionType.SNAPPY_COMPRESSION);
        //
        // options.setCompressionPerLevel(levelCompressList);
        //
        // options.setBottommostCompressionType(CompressionType.SNAPPY_COMPRESSION);

        if (!Files.isSymbolicLink(Paths.get(this.dbPath))) Files.createDirectories(Paths.get(this.dbPath));

        List<byte[]> cfList = RocksDB.listColumnFamilies(options, this.dbPath);
        if (cfList.size() > 0) {
            if (dbOpts == null) dbOpts = new DBOptions(options);
            dbOpts.setCreateMissingColumnFamilies(true);

            // open DB with two column families
            final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
                    new ArrayList<>();
            for (byte[] testCf: cfList) {
                // have to open default column family
                columnFamilyDescriptors.add(new ColumnFamilyDescriptor(testCf, new ColumnFamilyOptions()));
            }
            final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

            RocksDB tmpDB = RocksDB.open(dbOpts, this.dbPath, columnFamilyDescriptors, cfHandles);

            for (ColumnFamilyHandle cfHandle: cfHandles) {
                String cfName = new String(cfHandle.getName());
                cfHandlers.put(cfName, cfHandle);
            }

            return tmpDB;
        } else {
            return RocksDB.open(options, this.dbPath);
        }
    }

    public void createCfHandler(String cfName, ColumnFamilyOptions cfOpts)  throws RocksDBException {

        if (cfHandlers.get(cfName) == null) {
            ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(cfName.getBytes(StandardCharsets.UTF_8), cfOpts);

            ColumnFamilyHandle cfHandler = this.db.createColumnFamily(cfDescriptor);

            cfHandlers.put(cfName, cfHandler);
        }
    }

    public ColumnFamilyHandle getCfHandler(String cfName) throws RocksDBException {
        return cfHandlers.get(cfName);
    }

    public HashMap<String, ColumnFamilyHandle> getAllCfHandlers() { return this.cfHandlers; }

    public void flushDbBatch(WriteOptions writeOpt, WriteBatch batch) throws IOException, RocksDBException {
        if (db == null) this.db = getDBObject();

        this.db.write(writeOpt, batch);
    }

    public void flushDbBatch(WriteBatch batch, boolean disableWAL) throws IOException, RocksDBException {
        if (db == null) this.db = getDBObject();

        WriteOptions writeOpt = new WriteOptions();
        writeOpt.setDisableWAL(disableWAL);

        this.flushDbBatch(writeOpt, batch);

        writeOpt.close();
    }

    public void flushDbBatch(WriteBatch batch) throws IOException, RocksDBException {
        this.flushDbBatch(batch, false);
    }

    public void flushDb(byte[] hashKey, byte[] hashValue) throws RocksDBException, IOException {
        if (db == null) this.db = getDBObject();

        this.db.put(hashKey, hashValue);
    }

    public String get(String hashKey) throws RocksDBException, IOException {
        if (db == null) this.db = getDBObject();

        if (this.db.get(hashKey.getBytes(StandardCharsets.UTF_8)) != null) {
            return new String(this.db.get(hashKey.getBytes(StandardCharsets.UTF_8)));
        } else {
            return null;
        }
    }

    public void close() {
        if (db != null) {
            this.db.close();
        }

        if (dbOpts != null) dbOpts.close();

        if (options != null) options.close();

    }

}
