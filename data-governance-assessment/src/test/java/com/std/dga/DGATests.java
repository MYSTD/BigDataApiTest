package com.std.dga;

import com.std.dga.governance.bean.GovernanceAssessTable;
import com.std.dga.governance.service.*;
import com.std.dga.governance.service.impl.MainGovernanceServiceImpl;
import com.std.dga.meta.service.impl.TableMetaInfoServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Date;

@SpringBootTest
class DGATests {

    @Autowired
    TableMetaInfoServiceImpl tableMetaInfoService;
    @Autowired
    GovernanceAssessDetailService governanceAssessDetailService;

    @Autowired
    GovernanceAssessTableService governanceAssessTableService;
    @Autowired
    GovernanceAssessTecOwnerService governanceAssessTecOwnerService;
    @Autowired
    GovernanceAssessGlobalService governanceAssessGlobalService;

    @Autowired
    MainGovernanceService mainGovernanceService;

    @Test
    void contextLoads() {
    }

    @Test
    void testHiveClient() {
//        tableMetaInfoService.getHiveClient();
    }

    @Test
    void testInitTableMeta() throws Exception {
        tableMetaInfoService.initTableMeta("gmall","2023-05-02");
    }

    @Test
    void testGovernanceAssessDetailService() {
        governanceAssessDetailService.mainAssess("2023-05-02");
    }

    @Test
    void testGovernanceAssessTableService() {
        governanceAssessTableService.calcGovernanceAssessTable("2023-05-02");
    }
    @Test
    void testGovernanceAssessTecOwnerService() {
        governanceAssessTecOwnerService.calcAssessTecOwner("2023-05-02");
    }
    @Test
    void testGovernanceAssessGlobalService() {
        governanceAssessGlobalService.calcAssessGlobal("2023-05-02");
    }

    @Test
    void testMainGovernanceService() throws Exception {
        mainGovernanceService.startGovernance();
    }
    @Test
    void testDate(){
        System.out.println(new Date());
    }
}
