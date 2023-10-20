package com.std.dga.governance.service.impl;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.std.dga.governance.bean.GovernanceMetric;
import com.std.dga.governance.mapper.GovernanceMetricMapper;
import com.std.dga.governance.service.GovernanceMetricService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

/**
 * <p>
 * 考评指标参数表 服务实现类
 * </p>
 *
 * @author std
 * @since 2023-10-10
 */
@Service
@DS("dga")
public class GovernanceMetricServiceImpl extends ServiceImpl<GovernanceMetricMapper, GovernanceMetric> implements GovernanceMetricService {

}
