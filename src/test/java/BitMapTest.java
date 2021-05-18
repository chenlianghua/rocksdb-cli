import org.apache.coyote.OutputBuffer;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Date;

public class BitMapTest {
    public static void main(String[] args) {
        Roaring64NavigableMap roaring64NavigableMap = new Roaring64NavigableMap();

        // roaring64NavigableMap.addLong(3599000100000000L);

        System.out.println(roaring64NavigableMap.toString());

        RoaringBitmap bitmap1 = RoaringBitmap.bitmapOf(1, 2, 3, 4, 100, 101, 102, 103, 104, 3599000);
        bitmap1.add(23423);
        System.out.println(bitmap1.getSizeInBytes());

        // roaring64NavigableMap 持久化测试
        try {
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
