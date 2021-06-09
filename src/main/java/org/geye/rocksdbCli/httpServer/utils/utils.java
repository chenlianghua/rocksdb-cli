package org.geye.rocksdbCli.httpServer.utils;

import org.apache.commons.lang.time.FastDateFormat;

import java.io.File;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class utils {

    public static DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static String getBucket(long ts) {
        FastDateFormat DB_ROTATE_FORMAT = FastDateFormat.getInstance("yMMdd'h'HH");
        return DB_ROTATE_FORMAT.format(ts);
    }

    public static String currentTimeStr() {
        return sdf.format(new Date());
    }

    public static String addDays(int daysNum) {
        Calendar todayStart = Calendar.getInstance();
        todayStart.set(Calendar.HOUR_OF_DAY, 0);
        todayStart.set(Calendar.MINUTE, 0);
        todayStart.set(Calendar.SECOND, 0);
        todayStart.set(Calendar.MILLISECOND, 0);

        todayStart.add(Calendar.DATE, daysNum);

        return sdf.format(todayStart.getTime());
    }

    public static long dateTimeStr2Ts(String dateTimeStr) {
        return Timestamp.valueOf(dateTimeStr).getTime();
    }

    public static String buildKey(String dbPath, String indexType) {
        return String.format("%s:%s", dbPath, indexType);
    }


    public static long getMinBucketTs(String bucket) throws ParseException {
        return getBucketTs(bucket, "min");
    }

    public static long getMaxBucketTs(String bucket) throws ParseException {
        return getBucketTs(bucket, "max");
    }

    public static long getBucketTs(String bucket, String type) throws ParseException {
        SimpleDateFormat BUCKET_FORMAT = new SimpleDateFormat("yyMMdd'h'HHmmss");

        String newBucketStr = bucket;

        if (type.equals("max")) {
            newBucketStr += "5959";
        } else {
            newBucketStr += "0000";
        }

        Date date = BUCKET_FORMAT.parse(newBucketStr);

        long ts = date.getTime();
        if (type.equals("max")) {
            ts += 999;
        }

        return ts;
    }

    public static List<String> getAllBucketList() {

        File bucketDir = new File(Configs.BITMAP_HOME);
        List<String> bucketList = Arrays.asList(Objects.requireNonNull(bucketDir.list()));

        bucketList.sort(Collections.reverseOrder());
        return bucketList;
    }
}
