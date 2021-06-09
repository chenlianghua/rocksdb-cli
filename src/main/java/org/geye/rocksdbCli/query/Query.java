package org.geye.rocksdbCli.query;

import org.geye.rocksdbCli.bean.QueryParams;
import org.geye.rocksdbCli.httpServer.utils.Configs;
import org.rocksdb.RocksDB;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.geye.rocksdbCli.httpServer.utils.utils.getBucket;

public class Query {

    static {
        RocksDB.loadLibrary();
    }
    protected QueryParams params = new QueryParams();

    public Query() {};

    public Query(QueryParams params) {
        this.setParams(params);
    }

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
                // 8080 -> 08080
                // 443  -> 00443
                newVal = String.format("%05d", Integer.parseInt(raw));
                break;
            default:
                break;
        }

        return newVal;
    }

    public List<String> getBucketList() {
        List<String> bucketList = new ArrayList<>();
        long tmpTs = params.getStartTs();
        while (tmpTs <= params.getEndTs()) {
            String bucket = getBucket(tmpTs);

            bucketList.add(bucket);

            tmpTs += 3600 * 1000;
        }

        return bucketList;
    }

    private List<String> getDbPathList(String bucket, String type) {
        List<String> dbPathList = new ArrayList<>();
        String bucketPath = "";

        if (type.equals("index")) {
            bucketPath = Configs.INDEX_HOME + "/" + bucket;
        } else {
            bucketPath = Configs.SESSIONS_HOME + "/" + bucket;
        }

        File bucketDir = new File(bucketPath);

        if (bucketDir.exists()) {
            for (String subBucket: Objects.requireNonNull(bucketDir.list())) {
                String dbPath = bucketPath + "/" + subBucket;

                dbPathList.add(dbPath);
            }
        }

        return dbPathList.size() > 0 ? dbPathList : null;
    }

    public List<String> getIndexDdPathList(String bucket) {
        return this.getDbPathList(bucket, "index");
    }

    public List<String> getSessionDbPathList(String bucket) {
        return this.getDbPathList(bucket, "sessions");
    }

    public void setParams(QueryParams params) {
        this.params = params;
    }
}
