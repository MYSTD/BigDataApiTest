package com.std.dga.assessor.calc;

import com.std.dga.assessor.Assessor;
import com.std.dga.dolphinscheduler.bean.TDsTaskInstance;
import com.std.dga.dolphinscheduler.service.TDsTaskInstanceService;
import com.std.dga.governance.bean.AssessParam;
import com.std.dga.governance.bean.GovernanceAssessDetail;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;

@Component("TASK_FAILED")
public class TaskFailedAssessor extends Assessor {

    @Autowired
    TDsTaskInstanceService tDsTaskInstanceService ;

    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws Exception {
        String assessDate = assessParam.getAssessDate();
        String tableName = assessParam.getTableMetaInfo().getSchemaName()
                + "." + assessParam.getTableMetaInfo().getTableName();

        // 查询报错的任务
        List<TDsTaskInstance> failedTaskList = tDsTaskInstanceService.selectFailedTask(tableName, assessDate);

        if(failedTaskList.size() > 0 ){
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("该任务当日有报错");
        }


    }
}
