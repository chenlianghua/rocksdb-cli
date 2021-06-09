package org.geye.rocksdbCli.query.futures;

import org.geye.rocksdbCli.bean.QueryParams;
import org.geye.rocksdbCli.bean.RocksdbWithCF;
import org.geye.rocksdbCli.httpServer.cache.LRUCache;
import org.geye.rocksdbCli.httpServer.service.BitmapCacheInitService;
import org.geye.rocksdbCli.httpServer.utils.Configs;
import org.geye.rocksdbCli.httpServer.utils.utils;
import org.geye.rocksdbCli.query.Query;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.ParallelAggregation;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.rocksdb.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SearchBitmapTask extends Query {

    protected final String bitmapDbHome = Configs.BITMAP_HOME;
    protected final String cfPrefix = "bitmapCF-";

    protected final QueryParams params;
    protected final String bitmapDbPath;
    protected final String bucket;

    public SearchBitmapTask(String bucket, QueryParams queryParams) {
        this.bitmapDbPath = bitmapDbHome + "/" + bucket;
        this.params = queryParams;
        this.bucket = bucket;
    }

    public RocksIterator getIterator(RocksdbWithCF rocksObj) {
        return this.getIterator(rocksObj, "");
    }

    public String getCfName(String indexType) {
        return String.format("%s%s", this.cfPrefix, indexType);
    }

    public RocksIterator getIterator(RocksdbWithCF rocksObj, String prefixFilter) {
        ReadOptions readOptions = new ReadOptions();

        if (!prefixFilter.equals("")) {
            String lowerBound = String.format("%s@%s", prefixFilter, params.getStartTs());
            String upperBound = String.format("%s@%s", prefixFilter, params.getEndTs());

            readOptions.setIterateLowerBound(new Slice(lowerBound.getBytes(StandardCharsets.UTF_8)));
            readOptions.setIterateUpperBound(new Slice(upperBound.getBytes(StandardCharsets.UTF_8)));
        }

        return rocksObj.db.newIterator(rocksObj.cfHandler, readOptions);
    }

    public RocksdbWithCF getDb(String dbPath, String indexType) throws RocksDBException {

        LRUCache cache = BitmapCacheInitService.getCache();
        RocksdbWithCF rocksdbWithCF;
        rocksdbWithCF = cache.get(dbPath, indexType);
        if (rocksdbWithCF != null) {
            return rocksdbWithCF;
        }

        List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

        ColumnFamilyDescriptor defaultCfDescriptor = new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        cfDescriptors.add(defaultCfDescriptor);

        if (!indexType.equals(new String(RocksDB.DEFAULT_COLUMN_FAMILY))) {
            String cfName = this.getCfName(indexType);
            ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(cfName.getBytes(StandardCharsets.UTF_8));
            cfDescriptors.add(cfDescriptor);
        }

        RocksDB db = RocksDB.openReadOnly(dbPath, cfDescriptors, cfHandles);
        rocksdbWithCF = new RocksdbWithCF(db, cfHandles.get(cfHandles.size() - 1));

        cache.put(dbPath, indexType, rocksdbWithCF);

        return rocksdbWithCF;
    }

    /***
     * byte数组转成bitmap
     * @param inputByte byte[]
     * @return          bitmap
     * @throws IOException
     */
    public Roaring64Bitmap byte2Bitmap(byte[] inputByte) throws IOException {
        Roaring64Bitmap Roaring64Bitmap = new Roaring64Bitmap();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(inputByte);

        Roaring64Bitmap.deserialize(new DataInputStream(byteArrayInputStream));

        return Roaring64Bitmap;
    }

    /***
     * bitmap转成byte数组
     * @param bitmap    bitmap
     * @return          byte[]
     * @throws IOException
     */
    public byte[] bitmap2byte(Roaring64Bitmap bitmap) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        bitmap.serialize(dataOutputStream);

        return outputStream.toByteArray();
    }

    public Roaring64Bitmap fetchBitmap() throws ParseException {
        Roaring64Bitmap finalBitmap = new Roaring64Bitmap();

        long minBucketTs = utils.getMinBucketTs(bucket);
        long maxBucketTs = utils.getMaxBucketTs(bucket);

        long lowerBound = Math.max(0, (params.getStartTs() - minBucketTs) * 1000000);
        long highBound = (Math.min(maxBucketTs, params.getEndTs()) - minBucketTs) * 1000000 + 999999;

        long t1 = System.currentTimeMillis();
        // 过滤的字段: 5元组
        for (String indexType: params.getFilters().keySet()) {
            Roaring64Bitmap indexBitmap = new Roaring64Bitmap();
            try {
                RocksdbWithCF rocksObj = getDb(bitmapDbPath, indexType);

                String[] filterValues = params.getFilters().get(indexType);

                List<byte[]> filterBytes = new ArrayList<>();
                List<ColumnFamilyHandle> cfHandlerList = new ArrayList<>();

                for (int i=0; i<filterValues.length; i++) {
                    filterValues[i] = this.formatSearchTarget(indexType, filterValues[i]);
                    filterBytes.add(filterValues[i].getBytes(StandardCharsets.UTF_8));
                    cfHandlerList.add(rocksObj.cfHandler);
                }

                List<byte[]> resultBytes = rocksObj.db.multiGetAsList(cfHandlerList, filterBytes);
                for (byte[] bytes: resultBytes) {
                    try {
                        if (bytes != null) {
                            Roaring64Bitmap tmpBitmap = byte2Bitmap(bytes);

                            indexBitmap.or(tmpBitmap);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                if (finalBitmap.isEmpty()) {
                    finalBitmap = indexBitmap;
                } else {
                    finalBitmap.and(indexBitmap);
                }
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        }

        long t2 = System.currentTimeMillis();
        System.out.println(bitmapDbPath + " bitmap merge cost: " + (float) (t2 - t1) / 1000);

        return finalBitmap;
    }


}
