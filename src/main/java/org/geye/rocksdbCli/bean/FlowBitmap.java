package org.geye.rocksdbCli.bean;


import org.roaringbitmap.RoaringBitmap;

import java.io.*;

public class FlowBitmap {

    // 五元组内容
    public String flowKey = "";
    // 采用lastPacket时间戳为会话时间
    private long minSessionTs = 0L;
    private long maxSessionTs = 0L;

    public int increment = 0;
    public RoaringBitmap bitmap = new RoaringBitmap();

    public FlowBitmap(String flowKey) {
        this.flowKey = flowKey;
    }

    public FlowBitmap(String flowKey, byte[] inputByte) throws IOException {
        this.flowKey = flowKey;
        this.bitmap = byte2Bitmap(inputByte);
    }

    public void add(int indexId) {
        bitmap.add(indexId);
    }

    public String getKey() {
        return this.flowKey;
    }

    public void setBitmap(RoaringBitmap bitmap) {
        this.bitmap = bitmap;
    }

    public void or(FlowBitmap fbm) {
        this.bitmap.or(fbm.bitmap);
    }

    public void and(FlowBitmap fbm) {
        this.bitmap.and(fbm.bitmap);
    }

    /***
     * bitmap 转成byte数组
     * @return byte[]
     * @throws IOException
     */
    public byte[] toByte() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        this.bitmap.serialize(dataOutputStream);

        return outputStream.toByteArray();
    }

    /***
     * byte数组转成二进制
     * @param inputByte byte[]
     * @return bitmap
     * @throws IOException
     */
    public RoaringBitmap byte2Bitmap(byte[] inputByte) throws IOException {
        RoaringBitmap RoaringBitmap = new RoaringBitmap();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(inputByte);

        RoaringBitmap.deserialize(new DataInputStream(byteArrayInputStream));

        return RoaringBitmap;
    }

    public long getMinSessionTs() {
        return minSessionTs;
    }

    public void setMinSessionTs(long minSessionTs) {
        this.minSessionTs = Math.min(this.minSessionTs, minSessionTs);
    }

    public long getMaxSessionTs() {
        return maxSessionTs;
    }

    public void setMaxSessionTs(long maxSessionTs) {
        this.maxSessionTs = Math.max(this.maxSessionTs, maxSessionTs);
    }
}
