package org.geye.rocksdbCli.httpServer.utils.res.wrapper;

import com.alibaba.fastjson.JSONObject;

public class Success {

    JSONObject res = new JSONObject();

    public Success(String msg, Object data) {
        res.put("msg", msg);
        res.put("data", data);
    }

    @Override
    public String toString() {
        return this.res.toString();
    }
}
