package com.std.dga.governance.mapper;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.std.dga.governance.bean.GovernanceType;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/**
 * <p>
 * 治理考评类别权重表 Mapper 接口
 * </p>
 *
 * @author std
 * @since 2023-10-10
 */
@Mapper
@DS("dga")
public interface GovernanceTypeMapper extends BaseMapper<GovernanceType> {

}
