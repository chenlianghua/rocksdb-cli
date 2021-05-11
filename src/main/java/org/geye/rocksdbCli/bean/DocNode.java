package org.geye.rocksdbCli.bean;

import com.alibaba.fastjson.JSONObject;

public class DocNode{

    public final String k;
    public final String v;

    public DocNode(String k, String v) {
        this.k = k;
        this.v = v;
    }


    public String toString() {
        JSONObject json = new JSONObject();
        json.put("key", this.k);
        json.put("val", this.v);

        return json.toString();
    }
}
