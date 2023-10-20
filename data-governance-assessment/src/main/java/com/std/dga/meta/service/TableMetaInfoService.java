package com.std.dga.meta.service;

import com.std.dga.meta.bean.TableMetaInfo;
import com.baomidou.mybatisplus.extension.service.IService;
import com.std.dga.meta.bean.TableMetaInfoQuery;
import com.std.dga.meta.bean.TableMetaInfoVO;

import java.util.List;

/**
 * <p>
 * 元数据表 服务类
 * </p>
 *
 * @author std
 * @since 2023-10-07
 */
public interface TableMetaInfoService extends IService<TableMetaInfo> {

    void initTableMeta(String databaseName , String assessDate )throws Exception;

    List<TableMetaInfoVO> selectTableMetaInfoVOList(TableMetaInfoQuery tableMetaInfoQuery);
    Integer selectTableMetaInfoCount(TableMetaInfoQuery tableMetaInfoQuery);
}
