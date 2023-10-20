package com.std.dga.governance.service;

import com.std.dga.governance.bean.GovernanceAssessDetail;
import com.baomidou.mybatisplus.extension.service.IService;
import com.std.dga.governance.bean.GovernanceAssessDetailVO;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * 治理考评结果明细 服务类
 * </p>
 *
 * @author std
 * @since 2023-10-10
 */
public interface GovernanceAssessDetailService extends IService<GovernanceAssessDetail> {

    void mainAssess( String assessDate);

    List<GovernanceAssessDetailVO> getProblemList(String governanceType, Integer pageNo, Integer pageSize);

    Map<String, Long> getProblemNum();
}
