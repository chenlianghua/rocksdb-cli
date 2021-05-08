package org.geye.rocksdbCli.httpServer.utils.res.wrapper;

import com.alibaba.fastjson.JSONObject;

public class Error {

    JSONObject res = new JSONObject();

    public Error(String msg) {
        res.put("msg", msg);
    }

    @Override
    public String toString() {
        return this.res.toString();
    }

}
