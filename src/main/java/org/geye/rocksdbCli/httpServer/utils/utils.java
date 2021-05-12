package org.geye.rocksdbCli.httpServer.utils;

import org.apache.commons.lang.time.FastDateFormat;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

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

}
