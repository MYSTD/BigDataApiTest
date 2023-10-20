package com.std.dga.meta.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.google.gson.JsonObject;
import com.std.dga.meta.bean.TableMetaInfo;
import com.std.dga.meta.bean.TableMetaInfoExtra;
import com.std.dga.meta.bean.TableMetaInfoQuery;
import com.std.dga.meta.bean.TableMetaInfoVO;
import com.std.dga.meta.service.TableMetaInfoExtraService;
import com.std.dga.meta.service.TableMetaInfoService;
import org.apache.avro.data.Json;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.List;

/**
 * <p>
 * 元数据表 前端控制器
 * </p>
 *
 * @author std
 * @since 2023-10-07
 */
@RestController
@RequestMapping("/tableMetaInfo")
public class TableMetaInfoController {



    @Autowired
    TableMetaInfoService tableMetaInfoService;

    @Autowired
    TableMetaInfoExtraService tableMetaInfoExtraService;
    /**
     * 查询表信息列表
     *
     * 前端请求:
     * http://dga.gmall.com/tableMetaInfo/table-list?schemaName=gmall&tableName=user&dwLevel=ods&pageSize=20&pageNo=1
     */
    @GetMapping("/table-list")
    public String tableList(TableMetaInfoQuery tableMetaInfoQuery){

        System.out.println(tableMetaInfoQuery);

        List<TableMetaInfoVO> list = tableMetaInfoService.selectTableMetaInfoVOList(tableMetaInfoQuery);

        Integer count = tableMetaInfoService.selectTableMetaInfoCount(tableMetaInfoQuery);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("total",count);
        jsonObject.put("list",list);
        return jsonObject.toJSONString();

    }

    @GetMapping("/table/{tableMetaInfoId}")
    public TableMetaInfo listById(@PathVariable("tableMetaInfoId") Long tableMetaInfoId){
        TableMetaInfo tableMetaInfo = tableMetaInfoService.getById(tableMetaInfoId);

        TableMetaInfoExtra tableMetaInfoExtra = tableMetaInfoExtraService.getOne(
                new QueryWrapper<TableMetaInfoExtra>()
                        .eq("schema_name", tableMetaInfo.getSchemaName())
                        .eq("table_name", tableMetaInfo.getTableName())
        );
        tableMetaInfo.setTableMetaInfoExtra(tableMetaInfoExtra);

        return tableMetaInfo;
    }

    @PostMapping("/tableExtra")
    public String updateTableExtra(@RequestBody TableMetaInfoExtra tableMetaInfoExtra){
        tableMetaInfoExtra.setUpdateTime(new Date());
        if(tableMetaInfoExtraService.saveOrUpdate(tableMetaInfoExtra))
            return "success";
        else return "fail";
    }

    @PostMapping("init-tables/{schemaName}/{dt}")
    public String initTableMeta(@PathVariable("schemaName") String schemaName , @PathVariable("dt") String dt  ) throws Exception {
        tableMetaInfoService.initTableMeta(schemaName , dt);
        return "success" ;
    }

}
