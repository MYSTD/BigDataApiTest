package com.std.dga.dolphinscheduler.service;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.std.dga.dolphinscheduler.bean.TDsTaskDefinition;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author std
 * @since 2023-10-13
 */
@DS("dolphinscheduler")
public interface TDsTaskDefinitionService extends IService<TDsTaskDefinition> {

    List<TDsTaskDefinition> selectList();

}
