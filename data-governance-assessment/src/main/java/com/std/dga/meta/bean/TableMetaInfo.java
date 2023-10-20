package com.std.dga.meta.bean;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import java.io.Serializable;
import java.util.Date;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

/**
 * <p>
 * 元数据表
 * </p>
 *
 * @author std
 * @since 2023-10-07
 */
@Data
@TableName("table_meta_info")
public class TableMetaInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 表id
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * 表名
     */
    private String tableName;

    /**
     * 库名
     */
    private String schemaName;

    /**
     * 字段名json ( 来源:hive)
     */
    private String colNameJson;

    /**
     * 分区字段名json( 来源:hive)
     */
    private String partitionColNameJson;

    /**
     * hdfs所属人 ( 来源:hive)
     */
    private String tableFsOwner;

    /**
     * 参数信息 ( 来源:hive)
     */
    private String tableParametersJson;

    /**
     * 表备注 ( 来源:hive)
     */
    private String tableComment;

    /**
     * hdfs路径 ( 来源:hive)
     */
    private String tableFsPath;

    /**
     * 输入格式( 来源:hive)
     */
    private String tableInputFormat;

    /**
     * 输出格式 ( 来源:hive)
     */
    private String tableOutputFormat;

    /**
     * 行格式 ( 来源:hive)
     */
    private String tableRowFormatSerde;

    /**
     * 表创建时间 ( 来源:hive)
     */
    private Date tableCreateTime;

    /**
     * 表类型 ( 来源:hive)
     */
    private String tableType;

    /**
     * 分桶列 ( 来源:hive)
     */
    private String tableBucketColsJson;

    /**
     * 分桶个数 ( 来源:hive)
     */
    private Long tableBucketNum;

    /**
     * 排序列 ( 来源:hive)
     */
    private String tableSortColsJson;

    /**
     * 数据量大小 ( 来源:hdfs)
     */
    private Long tableSize=0L;

    /**
     * 所有副本数据总量大小  ( 来源:hdfs)
     */
    private Long tableTotalSize=0L;

    /**
     * 最后修改时间   ( 来源:hdfs)
     */
    private Date tableLastModifyTime;

    /**
     * 最后访问时间   ( 来源:hdfs)
     */
    private Date tableLastAccessTime;

    /**
     * 当前文件系统容量   ( 来源:hdfs)
     */
    private Long fsCapcitySize;

    /**
     * 当前文件系统使用量   ( 来源:hdfs)
     */
    private Long fsUsedSize;

    /**
     * 当前文件系统剩余量   ( 来源:hdfs)
     */
    private Long fsRemainSize;

    /**
     * 考评日期 
     */
    private String assessDate;

    /**
     * 创建时间 (自动生成)
     */
    private Date createTime;

    /**
     * 更新时间  (自动生成)
     */
    private Date updateTime;


    /**
     * 额外辅助信息
     */
    @TableField(exist = false)
    private TableMetaInfoExtra tableMetaInfoExtra;

}
