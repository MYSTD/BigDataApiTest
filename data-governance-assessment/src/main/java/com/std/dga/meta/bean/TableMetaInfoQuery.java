package com.std.dga.meta.bean;

import lombok.Data;

@Data
public class TableMetaInfoQuery {
    private String schemaName ;
    private String tableName;
    private String dwLevel ;
    private Integer pageSize ;
    private Integer pageNo ;
}
