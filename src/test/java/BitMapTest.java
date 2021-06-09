import com.google.common.cache.*;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressNetwork;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.ipv4.IPv4Address;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.ImmutableLongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.*;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import org.geye.rocksdbCli.httpServer.utils.utils;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import redis.clients.jedis.Jedis;
import sun.net.util.IPAddressUtil;

public class BitMapTest {

    @Test
    public void bitmapSerializeTest() throws IOException {
        // roaring64NavigableMap 持久化测试
        Roaring64NavigableMap roaring64NavigableMap = new Roaring64NavigableMap();

        roaring64NavigableMap.addLong(224721000002422L);
        roaring64NavigableMap.addLong(1278318000005672L);

        System.out.println("before: " + roaring64NavigableMap);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        roaring64NavigableMap.serialize(dataOutputStream);

        Roaring64NavigableMap roaring64NavigableMap2 = new Roaring64NavigableMap();

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(outputStream.toByteArray());

        roaring64NavigableMap2.deserialize(new DataInputStream(byteArrayInputStream));

        System.out.println("after: " + roaring64NavigableMap2);
    }

    @Test
    public void bucketTsCalculateTest() throws ParseException {
        String bucket = "210608h19";
        long startBucketTs = utils.getMinBucketTs(bucket);
        long endBucketTs = utils.getMaxBucketTs(bucket);

        long currentTs = System.currentTimeMillis();

        System.out.println(currentTs - startBucketTs);
        System.out.println("lower bound    : " + (currentTs - startBucketTs) * 1000000);
        System.out.println("high  bound    : " + ((currentTs - startBucketTs) * 1000000 + 999999));

        System.out.println("bucket: " + bucket);
        System.out.println("current ts     : " + currentTs);
        System.out.println("bucket first ts: " + startBucketTs);
        System.out.println("bucket  last ts: " + endBucketTs);

    }

    @Test
    public void roaringBitmapTest() {
        Roaring64NavigableMap roaring64NavigableMap1 = new Roaring64NavigableMap();
        Roaring64NavigableMap roaring64NavigableMap2 = new Roaring64NavigableMap();

        HashMap<Long, Integer> cache = new HashMap<>();

        for (long i=1; i<=1000; i++) {
            long start = System.currentTimeMillis();
            long tmp = start + i;

            if (tmp % 2 == 0) {
                roaring64NavigableMap1.add(tmp);
                cache.put(tmp, 1);
            }
            if (tmp % 3 == 0) roaring64NavigableMap2.add(tmp);
        }

        System.out.println("r1 size: " + roaring64NavigableMap1.getLongSizeInBytes() / 1024);
        System.out.println("r2 size: " + roaring64NavigableMap2.getLongSizeInBytes() / 1024);

        System.out.println("r1 count: " + roaring64NavigableMap1.getLongCardinality());
        System.out.println("r2 count: " + roaring64NavigableMap2.getLongCardinality());

        long st = System.currentTimeMillis();
        for (Integer v: cache.values()) {
            // System.out.println(item);
            v += 1;
        }
        long et = System.currentTimeMillis();
        float mapCost = (float) (et - st) / 1000;
        System.out.println("cache key set iterate cost: " + mapCost);

        long lt1 = System.currentTimeMillis();
        for (Iterator<Long> it = roaring64NavigableMap1.iterator(); it.hasNext(); ) {
            long item = it.next();
            // System.out.println(item);
            cache.get(item);
        }
        long lt2 = System.currentTimeMillis();
        float cost1 = (float) (lt2 - lt1) / 1000;
        System.out.println("r1 iterate cost: " + cost1);

        long t1 = System.currentTimeMillis();
        roaring64NavigableMap1.or(roaring64NavigableMap2);
        long t2 = System.currentTimeMillis();

        float cost = (float) (t2 - t1) / 1000;
        System.out.println(cost);

    }

    @Test
    public void roaringBitmapRangeTest() {
        RoaringBitmap roaringBitmap = new RoaringBitmap();
        System.out.println(roaringBitmap.limit(10));

        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        PrimitiveIterator.OfLong iterator = bitmap.stream().limit(20).iterator();
        while (iterator.hasNext()) {
            Long v = iterator.next();
            System.out.println(v);
        }
    }

    @Test
    public void IPAddressUtilsTest() {
        Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
        Roaring64NavigableMap bitmap2 = new Roaring64NavigableMap();

        String[] ipList = {"10.0.0.1", "10.0.0.2", "192.168.10.1", "172.16.1.10", "3.3.3.3"};
        for (String ip: ipList) {
            IPAddressString ipAddressString = new IPAddressString(ip);

            BigInteger value = ipAddressString.getAddress().getValue();
            bitmap.add(value.longValue());
        }
        System.out.println(bitmap);

        String[] ipList2 = {"10.0.0.1", "10.0.0.3", "192.168.10.1", "1.1.1.1", "2.2.2.2"};
        for (String ip: ipList2) {
            IPAddressString ipAddressString = new IPAddressString(ip);

            BigInteger value = ipAddressString.getAddress().getValue();
            bitmap2.add(value.longValue());
        }
        System.out.println(bitmap2);

        bitmap.andNot(bitmap2);
        System.out.println(bitmap);


        System.out.println("----------------------------------");
        for (Iterator<Long> it = bitmap.iterator(); it.hasNext(); ) {
            long item = it.next();
            System.out.println(item);
        }

        System.out.println(bitmap.contains(new IPAddressString("10.0.0.1").getAddress().getValue().longValue()));
        System.out.println(bitmap.contains(new IPAddressString("10.0.0.2").getAddress().getValue().longValue()));
        System.out.println(bitmap.contains(new IPAddressString("10.0.0.3").getAddress().getValue().longValue()));
        System.out.println(bitmap.contains(new IPAddressString("192.168.10.4").getAddress().getValue().longValue()));
    }
}
