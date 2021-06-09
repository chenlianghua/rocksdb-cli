package org.geye.rocksdbCli.httpServer.controller;

import com.alibaba.fastjson.JSONObject;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.geye.rocksdbCli.bean.DocNode;
import org.geye.rocksdbCli.bean.QueryParams;
import org.geye.rocksdbCli.httpServer.controller.dto.SearchDTO;
import org.geye.rocksdbCli.httpServer.utils.res.wrapper.Success;
import org.geye.rocksdbCli.httpServer.utils.utils;
import org.geye.rocksdbCli.query.Search;
import org.springframework.web.bind.annotation.*;

import java.util.List;


@Api(tags = "查询接口")
@RestController
@RequestMapping(value = "_search")
public class SearchController {

    @ApiOperation(value = "数据查询", notes = "数据查询")
    @PostMapping("")
    public Object post(
            @RequestBody SearchDTO searchDTO
            )
    {
        String srcIp = searchDTO.getSrcIp();
        String dstIp = searchDTO.getDstIp();
        String protocol = searchDTO.getProtocol();
        String srcPort = searchDTO.getSrcPort();
        String dstPort = searchDTO.getDstPort();

        long startTs = utils.dateTimeStr2Ts(searchDTO.getStartTime());
        long endTs = utils.dateTimeStr2Ts(searchDTO.getEndTime());
        int limit = searchDTO.getLimit();

        QueryParams queryParams = new QueryParams(startTs, endTs, limit);
        queryParams.set("srcIp", srcIp);
        queryParams.set("dstIp", dstIp);
        queryParams.set("protocol", protocol);
        queryParams.set("srcPort", srcPort);
        queryParams.set("dstPort", dstPort);

        Search search = new Search(queryParams);

        long t1 = System.currentTimeMillis();
        List<JSONObject> result = search.doQuery().result();
        long t2 = System.currentTimeMillis();

        return new Success("查询成功，耗时: " + (float) (t2 - t1) / 1000 + "秒", result);
    }

}
