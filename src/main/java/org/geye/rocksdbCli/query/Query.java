package org.geye.rocksdbCli.query;

import org.apache.commons.lang.time.FastDateFormat;
import org.geye.rocksdbCli.bean.QueryParams;
import org.rocksdb.RocksDB;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public abstract class Query {

    static {
        RocksDB.loadLibrary();
    }
    protected final QueryParams params = new QueryParams();

    protected String dbHome = "/data0/rocksdb";
    protected String indexHome = String.format("%s/index", dbHome);
    protected String sessionHome = String.format("%s/sessions", dbHome);
    protected FastDateFormat tsFormatter = FastDateFormat.getInstance("yMMdd'h'HH");

    protected String formatSearchTarget(String indexType, String raw) {
        String newVal = raw;
        switch (indexType) {
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

    protected List<String> getBucketList() {
        List<String> bucketList = new ArrayList<>();
        long tmpTs = params.getStartTs();
        while (tmpTs <= params.getEndTs()) {
            String bucket = tsFormatter.format(tmpTs);

            bucketList.add(bucket);

            tmpTs += 3600 * 1000;
        }

        return bucketList;
    }

    protected List<String> getIndexDdPathList(String bucket) {
        List<String> dbPathList = new ArrayList<>();

        String bucketPath = this.indexHome + "/" + bucket;
        File bucketDir = new File(bucketPath);

        if (bucketDir.exists()) {
            for (String subBucket: Objects.requireNonNull(bucketDir.list())) {
                String dbPath = bucketPath + "/" + subBucket;

                dbPathList.add(dbPath);
            }
        }

        return dbPathList.size() > 0 ? dbPathList : null;
    }

}
