package org.geye.rocksdbCli.bean;

import com.alibaba.fastjson.JSONObject;

public class DocNode{

    public final String index;
    public final String sessionDb;
    public final String k;
    public final JSONObject v;

    public DocNode(String k, JSONObject v, String index, String sessionDb) {
        this.k = k;
        this.v = v;
        this.index = index;
        this.sessionDb = sessionDb;
    }


    public String toString() {
        JSONObject json = new JSONObject();
        json.put("key", this.k);
        json.put("val", this.v);
        json.put("index", this.index);
        json.put("sessionDb", this.sessionDb);

        return json.toString();
    }
}
