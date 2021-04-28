
import org.apache.commons.lang.time.FastDateFormat;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


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

    private RocksDB getDBHandler(String dbPath) throws RocksDBException {
        return RocksDB.openReadOnly(dbPath);
    }

    private RocksDB getDBHandler(String dbPath, String columnFamily) throws RocksDBException {

        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

        ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(columnFamily.getBytes(StandardCharsets.UTF_8));
        columnFamilyDescriptors.add(columnFamilyDescriptor);

        return RocksDB.openReadOnly(dbPath, columnFamilyDescriptors, columnFamilyHandles);
    }

    public void doSearch(long startTs, long endTs, String indexType, String target, int limit) {
        target = formatSearchTarget(indexType, target);
        System.out.println(target);

        String startBucket = tsFormatter.format(startTs);
        String endBucket = tsFormatter.format(endTs);

        List<String> bucketList = new ArrayList<>();

        long tmpTs = startTs;
        while (tmpTs <= endTs) {
            String bucket = tsFormatter.format(tmpTs);
            bucketList.add(bucket);

            System.out.println(tmpTs);

            tmpTs += 3600 * 1000;
        }

        System.out.println(String.join(",", bucketList));
    }

}
