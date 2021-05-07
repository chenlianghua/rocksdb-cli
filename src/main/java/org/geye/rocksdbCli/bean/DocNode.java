package org.geye.rocksdbCli.bean;

import com.alibaba.fastjson.JSONObject;

public class DocNode{

    private final String k;
    private final String v;

    public DocNode(String k, String v) {
        this.k = k;
        this.v = v;
    }

    public String getK() {
        return k;
    }

    public String getV() {
        return v;
    }


    public String toString() {
        JSONObject json = new JSONObject();
        json.put("key", this.getK());
        json.put("val", this.getV());

        return json.toString();
    }
}
