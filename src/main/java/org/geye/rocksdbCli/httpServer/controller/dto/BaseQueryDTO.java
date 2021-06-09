package org.geye.rocksdbCli.httpServer.controller.dto;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.geye.rocksdbCli.httpServer.utils.utils;

@Data
@ApiModel(value = "基本过滤参数")
public class BaseQueryDTO {

    @ApiModelProperty(value = "expression", required = true)
    String expression;

    @ApiModelProperty(value = "srcIp", required = false)
    String srcIp = "";

    @ApiModelProperty(value = "dstIp", required = false)
    String dstIp = "";

    @ApiModelProperty(value = "srcPort", required = false)
    String srcPort = "";

    @ApiModelProperty(value = "dstPort", required = false)
    String dstPort = "";

    @ApiModelProperty(value = "protocol", required = false)
    String protocol = "";

    @ApiModelProperty(value = "startTime")
    String startTime = utils.addDays(-30);

    @ApiModelProperty(value = "endTime")
    String endTime = utils.currentTimeStr();

}
