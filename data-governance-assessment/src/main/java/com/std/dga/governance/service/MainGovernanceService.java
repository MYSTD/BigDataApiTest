package com.std.dga.governance.service;

/**
 * ClassName:MainGovernanceService
 * Description:
 *
 * @date:2023/10/16 14:12
 * @author:STD
 */
public interface MainGovernanceService {
    void startGovernance() throws Exception;
    void restartGovernance(String assessDate) throws Exception;
}
