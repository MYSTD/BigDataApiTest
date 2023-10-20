package com.std.dga.meta.service;

import com.std.dga.meta.bean.TableMetaInfo;
import com.std.dga.meta.bean.TableMetaInfoExtra;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

/**
 * <p>
 * 元数据表附加信息 服务类
 * </p>
 *
 * @author std
 * @since 2023-10-07
 */
public interface TableMetaInfoExtraService extends IService<TableMetaInfoExtra> {

    void initTableMetaExtra(List<TableMetaInfo> tableMetaInfoList);
}
