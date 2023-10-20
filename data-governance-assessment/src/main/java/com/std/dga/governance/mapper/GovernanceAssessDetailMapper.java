package com.std.dga.governance.mapper;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.std.dga.governance.bean.GovernanceAssessDetail;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/**
 * <p>
 * 治理考评结果明细 Mapper 接口
 * </p>
 *
 * @author std
 * @since 2023-10-10
 */
@Mapper
@DS("dga")
public interface GovernanceAssessDetailMapper extends BaseMapper<GovernanceAssessDetail> {

}
