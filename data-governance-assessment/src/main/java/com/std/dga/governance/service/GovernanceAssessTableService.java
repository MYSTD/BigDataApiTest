package com.std.dga.governance.service;

import com.std.dga.governance.bean.GovernanceAssessTable;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 表治理考评情况 服务类
 * </p>
 *
 * @author std
 * @since 2023-10-16
 */
public interface GovernanceAssessTableService extends IService<GovernanceAssessTable> {

    public void  calcGovernanceAssessTable(String assessDate);
}
