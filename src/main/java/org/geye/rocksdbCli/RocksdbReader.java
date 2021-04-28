package org.geye.rocksdbCli;

import org.apache.commons.lang.time.FastDateFormat;
import org.rocksdb.*;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


public class RocksdbReader {
    static {
        RocksDB.loadLibrary();
    }

    private String dbHome = "/data0/rocksdb";
    private String indexHome = String.format("%s/index", dbHome);
    private String sessionHome = String.format("%s/sessions", dbHome);
    private FastDateFormat tsFormatter = FastDateFormat.getInstance("yMMdd'h'HH");

    private String formatSearchTarget(String field, String raw) {
        String newVal = raw;
        switch (field) {
            case "protocol":
                // http -> 000000http
                // icmp -> 000000icmp
                // dns  -> 0000000dns
                if (raw.length() < 10) {
                    newVal = new String(new char[10 - raw.length()]).replace('\0', '0') + raw;
                }
                break;
            case "srcIp":
            case "dstIp":
                // 10.0.0.1 -> 010.000.000.001
                // 192.168.10.1 -> 192.168.010.001
                String[] subItemArr = raw.split("\\.");
                for (int j = 0; j < subItemArr.length; j++) {
                    String subItem = subItemArr[j];
                    if (subItem.length() < 3) {
                        subItemArr[j] = new String(new char[3 - subItem.length()]).replace('\0', '0') + subItem;
                    }
                }
                newVal = String.join(".", subItemArr);
                break;
            case "srcPort":
            case "dstPort":
                // 8080 -> 008080
                // 443  -> 000443
                newVal = String.format("%06d", Integer.parseInt(raw));
                break;
            default:
                break;
        }

        return newVal;
    }

    public void readSingleDb(String dbPath, String indexType, String target, long startTs, long endTs, int limit) {
        try {
            String cfName = String.format("flowIndexCF-%s", indexType);
            String filter = String.format("%s%s", indexType, target);

            List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
            List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

            ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(cfName.getBytes(StandardCharsets.UTF_8));
            cfDescriptors.add(cfDescriptor);

            String lowerBound = String.format("%s:%s:%s", filter, startTs, "0000000000000");
            String upperBound = String.format("%s:%s:%s", filter, endTs, System.nanoTime());

            RocksDB db = RocksDB.openReadOnly(dbPath, cfDescriptors, cfHandles);
            ReadOptions rOpts = new ReadOptions();

            rOpts.setIterateLowerBound(new Slice(lowerBound.getBytes(StandardCharsets.UTF_8), filter.length()));
            rOpts.setIterateUpperBound(new Slice(upperBound.getBytes(StandardCharsets.UTF_8), filter.length()));

            RocksIterator iterator = db.newIterator();
            iterator.seek(target.getBytes(StandardCharsets.UTF_8));

            int cnt = 0;
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                iterator.status();

                try {
                    byte[] keyBytes = iterator.key();
                    byte[] valueBytes = iterator.value();

                    assert (iterator.key() != null);
                    assert (iterator.value() != null);

                    String key = new String(keyBytes);
                    String value = new String(valueBytes);

                    System.out.println(String.join(",", dbPath, key, value));
                    cnt++;

                    if (cnt >= limit) {
                        break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public void doSearch(long startTs, long endTs, String indexType, String target, int limit) {
        target = formatSearchTarget(indexType, target);
        System.out.println(target);

//        String startBucket = tsFormatter.format(startTs);
//        String endBucket = tsFormatter.format(endTs);

        List<String> bucketList = new ArrayList<>();

        long tmpTs = startTs;
        while (tmpTs <= endTs) {
            String bucket = tsFormatter.format(tmpTs);
            bucketList.add(bucket);

            System.out.println(tmpTs);

            tmpTs += 3600 * 1000;
        }

        System.out.println(String.join(",", bucketList));

        for (String bucket: bucketList) {
            String bucketPath = this.indexHome + "/" + bucket;
            File bucketDir = new File(bucketPath);
            for (String subBucket: Objects.requireNonNull(bucketDir.list())) {
                String dbPath = bucketPath + "/" + subBucket;
                readSingleDb(dbPath, indexType, target, startTs, endTs, limit);
            }
        }
    }
}
