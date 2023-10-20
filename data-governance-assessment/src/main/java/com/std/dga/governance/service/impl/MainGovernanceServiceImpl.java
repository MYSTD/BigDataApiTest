package com.std.dga.governance.service.impl;

import com.std.dga.governance.service.*;
import com.std.dga.meta.service.TableMetaInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;


/**
 * ClassName:MainGovernanceServiceImpl
 * Description:
 *
 * @date:2023/10/16 14:29
 * @author:STD
 */
@Service
public class MainGovernanceServiceImpl implements MainGovernanceService {

    @Autowired
    TableMetaInfoService tableMetaInfoService;

    @Autowired
    GovernanceAssessDetailService governanceAssessDetailService;

    @Autowired
    GovernanceAssessTableService governanceAssessTableService;

    @Autowired
    GovernanceAssessTecOwnerService governanceAssessTecOwnerService;

    @Autowired
    GovernanceAssessGlobalService governanceAssessGlobalService;

    @Value("${default.assess.schema}")
    private String defaultAssessSchema;

    /**
     * 治理的整体任务流程 ， 整个治理的批处理部分。
     * <p>
     * 该方法是定时调度执行。
     * <p>
     * Spring提供的轻量的定时调度功能 SpringTask
     * <p>
     * Linux crontab :  * * * * * *
     */
    @Scheduled(cron = "* 18 14 * * *")
    @Override
    public void startGovernance() throws Exception {
        // 取当前日期作为考评日期
        //Date currentDate = new Date();
        //String assessDate = DateFormatUtils.format(currentDate, "yyyy-MM-dd");

        String assessDate = "2023-05-02";

        // 提取元数据
        tableMetaInfoService.initTableMeta(defaultAssessSchema, assessDate);

        // 考评
        governanceAssessDetailService.mainAssess(assessDate);

        // 核分
        governanceAssessTableService.calcGovernanceAssessTable(assessDate);
        governanceAssessTecOwnerService.calcAssessTecOwner(assessDate);
        governanceAssessGlobalService.calcAssessGlobal(assessDate);
    }

    @Override
    public void restartGovernance(String assessDate) throws Exception {

        // 提取元数据
        tableMetaInfoService.initTableMeta(defaultAssessSchema, assessDate);

        // 考评
        governanceAssessDetailService.mainAssess(assessDate);

        // 核分
        governanceAssessTableService.calcGovernanceAssessTable(assessDate);
        governanceAssessTecOwnerService.calcAssessTecOwner(assessDate);
        governanceAssessGlobalService.calcAssessGlobal(assessDate);

    }
}
