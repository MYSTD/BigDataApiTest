package com.std.dga.governance.service;

import com.std.dga.governance.bean.GovernanceAssessGlobal;
import com.baomidou.mybatisplus.extension.service.IService;
import org.apache.commons.math3.analysis.function.Max;

import java.util.Map;

/**
 * <p>
 * 治理总考评表 服务类
 * </p>
 *
 * @author std
 * @since 2023-10-16
 */
public interface GovernanceAssessGlobalService extends IService<GovernanceAssessGlobal> {

    public void calcAssessGlobal(String assessDate);

    Map<String,Object> getLastGlobalScore();
}
