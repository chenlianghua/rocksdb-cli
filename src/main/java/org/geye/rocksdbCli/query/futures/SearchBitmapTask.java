package org.geye.rocksdbCli.query.futures;

import com.github.benmanes.caffeine.cache.Cache;
import org.geye.rocksdbCli.bean.QueryParams;
import org.geye.rocksdbCli.bean.RocksdbWithCF;
import org.geye.rocksdbCli.httpServer.service.BitmapCacheInitService;
import org.geye.rocksdbCli.httpServer.utils.Configs;
import org.geye.rocksdbCli.httpServer.utils.utils;
import org.geye.rocksdbCli.query.Query;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.ParallelAggregation;
import org.roaringbitmap.RoaringBitmap;
import org.rocksdb.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
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

    public String getCfName(String indexType) {
        return String.format("%s%s", this.cfPrefix, indexType);
    }

    public RocksIterator getIterator(RocksdbWithCF rocksObj) {
        return this.getIterator(rocksObj, "");
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

        String cacheKey = utils.buildKey(dbPath, indexType);

        Cache<String, RocksdbWithCF> cache = BitmapCacheInitService.getCache();
        RocksdbWithCF rocksdbWithCF;
        rocksdbWithCF = cache.getIfPresent(cacheKey);
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

        cache.put(cacheKey, rocksdbWithCF);

        return rocksdbWithCF;
    }

    /***
     * byte数组转成bitmap
     * @param inputByte byte[]
     * @return          bitmap
     * @throws IOException
     */
    public RoaringBitmap byte2Bitmap(byte[] inputByte) throws IOException {
        RoaringBitmap RoaringBitmap = new RoaringBitmap();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(inputByte);

        RoaringBitmap.deserialize(new DataInputStream(byteArrayInputStream));

        return RoaringBitmap;
    }

    /***
     * bitmap转成byte数组
     * @param bitmap    bitmap
     * @return          byte[]
     * @throws IOException
     */
    public byte[] bitmap2byte(RoaringBitmap bitmap) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        bitmap.serialize(dataOutputStream);

        return outputStream.toByteArray();
    }

    public RoaringBitmap fetchBitmap() throws ParseException {
        long t1 = System.currentTimeMillis();

        RoaringBitmap finalBitmap = new RoaringBitmap();
        long minBucketTs = utils.getMinBucketTs(bucket);
        long maxBucketTs = utils.getMaxBucketTs(bucket);

        int lowerBound = Math.max(0, (int) (params.getStartTs() - minBucketTs) * 100);
        int highBound = ((int) (Math.min(maxBucketTs, params.getEndTs()) - minBucketTs)) * 100 + 99;

        finalBitmap.add(lowerBound, highBound);

        // 过滤的字段: 5元组
        List<RoaringBitmap> flowBitmapList = new ArrayList<>();
        for (String indexType: params.getFilters().keySet()) {
            // RoaringBitmap indexBitmap = new RoaringBitmap();
            long tt1 = System.currentTimeMillis();

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
                List<RoaringBitmap> tmpBitmapList = new ArrayList<>();

                RoaringBitmap indexBitmap = new RoaringBitmap();
                for (byte[] bytes: resultBytes) {
                    try {
                        if (bytes != null) {
                            RoaringBitmap tmpBitmap = byte2Bitmap(bytes);

                            // indexBitmap.or(tmpBitmap);

                            tmpBitmapList.add(tmpBitmap);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (tmpBitmapList.size() > 0) {
                    RoaringBitmap[] tmpBitmapArr = tmpBitmapList.toArray(new RoaringBitmap[tmpBitmapList.size()]);
                    indexBitmap = ParallelAggregation.or(tmpBitmapArr);
                }
                flowBitmapList.add(indexBitmap);

                // if (finalBitmap.isEmpty()) {
                //     finalBitmap = indexBitmap;
                // } else {
                //     finalBitmap.and(indexBitmap);
                // }

            } catch (RocksDBException e) {
                e.printStackTrace();
            }
            long tt2 = System.currentTimeMillis();

            System.out.println(bitmapDbPath + " flow bitmap merge cost: " + (float) (tt2 - tt1) / 1000);
        }

        if (flowBitmapList.size() > 0) {
            RoaringBitmap[] flowBitmapArr = flowBitmapList.toArray(new RoaringBitmap[flowBitmapList.size()]);
            RoaringBitmap tmpFinalBitmap = FastAggregation.and(flowBitmapArr);

            finalBitmap.and(tmpFinalBitmap);
        }

        long t2 = System.currentTimeMillis();
        System.out.println(bitmapDbPath + " bitmap merge cost: " + (float) (t2 - t1) / 1000);

        return finalBitmap;
    }

    public RoaringBitmap fetchBitmap(String indexType) throws RocksDBException {
        RocksdbWithCF rocksdb = this.getDb(bitmapDbPath, indexType);

        RocksIterator iterator = this.getIterator(rocksdb);

        RoaringBitmap finalBitmap = new RoaringBitmap();
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            byte[] keyBytes = iterator.key();
            byte[] bytes = iterator.value();

            try {
                String key = new String(keyBytes);
                RoaringBitmap tmpBitmap = byte2Bitmap(bytes);

                finalBitmap.or(tmpBitmap);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        iterator.close();

        return finalBitmap;
    }
}
