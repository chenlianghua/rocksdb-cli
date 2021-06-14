package org.geye.rocksdbCli.httpServer.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.geye.rocksdbCli.bean.QueryParams;
import org.geye.rocksdbCli.httpServer.utils.Configs;
import org.geye.rocksdbCli.httpServer.utils.res.wrapper.Success;
import org.geye.rocksdbCli.httpServer.utils.utils;
import org.geye.rocksdbCli.query.futures.SearchBitmapTask;
import org.roaringbitmap.RoaringBitmap;
import org.rocksdb.RocksDBException;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.text.ParseException;

@Api(tags = "查询接口")
@RestController
@RequestMapping(value = "/bitmap/check")
public class BitmapController {

    @ApiOperation(value = "数据查询", notes = "数据查询")
    @PutMapping("/{bucket}")
    public Object put(@PathVariable String bucket) throws ParseException, RocksDBException {
        long t1 = System.currentTimeMillis();

        String bucketBitmapDbPath = Configs.BITMAP_HOME + "/" + bucket;
        File bucketDir = new File(bucketBitmapDbPath);
        if (!bucketDir.exists()) {
            return new Error(bucket + " doesn't exist!");
        }

        long minBucketTs = utils.getMinBucketTs(bucket);
        long maxBucketTs = utils.getMaxBucketTs(bucket);

        QueryParams queryParams = new QueryParams();
        queryParams.setStartTs(minBucketTs);
        queryParams.setEndTs(maxBucketTs);

        SearchBitmapTask searchBitmapTask = new SearchBitmapTask(bucket, queryParams);
        RoaringBitmap protocolBitmap = searchBitmapTask.fetchBitmap("protocol");
        RoaringBitmap srcPortBitmap = searchBitmapTask.fetchBitmap("srcPort");
        RoaringBitmap dstPortBitmap = searchBitmapTask.fetchBitmap("dstPort");

        System.out.println("protocol: " + protocolBitmap.getLongCardinality());
        System.out.println("src port: " + srcPortBitmap.getLongCardinality());
        System.out.println("dst port: " + dstPortBitmap.getLongCardinality());

        long t2 = System.currentTimeMillis();

        return new Success("查询成功，耗时: " + (float) (t2 - t1) / 1000 + "秒", true);
    }

}
