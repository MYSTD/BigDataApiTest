package com.std.dga.governance.service.impl;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.std.dga.governance.bean.GovernanceType;
import com.std.dga.governance.mapper.GovernanceTypeMapper;
import com.std.dga.governance.service.GovernanceTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

/**
 * <p>
 * 治理考评类别权重表 服务实现类
 * </p>
 *
 * @author std
 * @since 2023-10-10
 */
@Service
@DS("dga")
public class GovernanceTypeServiceImpl extends ServiceImpl<GovernanceTypeMapper, GovernanceType> implements GovernanceTypeService {

}
