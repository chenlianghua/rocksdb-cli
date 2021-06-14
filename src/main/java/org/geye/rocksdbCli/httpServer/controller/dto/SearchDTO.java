package org.geye.rocksdbCli.httpServer.controller.dto;


import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@Data
@ApiModel(value = "查询操作需要用的参数")
public class SearchDTO extends BaseQueryDTO {

    @ApiModelProperty(value = "limit", allowEmptyValue = true)
    int limit = 10;

}
