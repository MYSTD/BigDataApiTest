package com.std.dga.governance.service;

import com.std.dga.governance.bean.GovernanceAssessTecOwner;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * 技术负责人治理考评表 服务类
 * </p>
 *
 * @author std
 * @since 2023-10-16
 */
public interface GovernanceAssessTecOwnerService extends IService<GovernanceAssessTecOwner> {

    public void calcAssessTecOwner(String assessDate);

    public List<Map<String,Object>> getRankList();
}
