package com.std.dga.dolphinscheduler.mapper;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.std.dga.dolphinscheduler.bean.TDsTaskDefinition;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author std
 * @since 2023-10-13
 */
@Mapper
@DS("dolphinscheduler")
public interface TDsTaskDefinitionMapper extends BaseMapper<TDsTaskDefinition> {

}
