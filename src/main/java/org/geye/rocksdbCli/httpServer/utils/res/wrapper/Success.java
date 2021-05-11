package org.geye.rocksdbCli.httpServer.utils.res.wrapper;

import com.alibaba.fastjson.JSONObject;

public class Success {

    public String msg;
    public Object data;

    public Success(String msg, Object data) {
        this.msg = msg;
        this.data = data;
    }

    @Override
    public String toString() {
        JSONObject res = new JSONObject();

        res.put("msg", this.msg);
        res.put("data", this.data);

        return res.toString();
    }
}
