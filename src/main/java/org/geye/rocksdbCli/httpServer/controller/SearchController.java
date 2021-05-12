package org.geye.rocksdbCli.httpServer.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.geye.rocksdbCli.bean.DocNode;
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

        String expression = searchDTO.getExpression();
        long startTs = utils.dateTimeStr2Ts(searchDTO.getStartTime());
        long endTs = utils.dateTimeStr2Ts(searchDTO.getEndTime());
        int limit = searchDTO.getLimit();

        String[] expressionArr = expression.split("==");

        String indexType = expressionArr[0].trim();
        String target = expressionArr[1].trim();

        Search search = new Search(startTs, endTs, indexType, target, limit);

        long t1 = System.currentTimeMillis();
        List<DocNode> result = search.doQuery().result();
        long t2 = System.currentTimeMillis();

        return new Success("查询成功，耗时: " + (float) (t2 - t1) / 1000 + "秒", result);
    }

}
