package com.std.dga.assessor.quality;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.std.dga.assessor.Assessor;
import com.std.dga.dolphinscheduler.bean.TDsTaskInstance;
import com.std.dga.dolphinscheduler.service.TDsTaskInstanceService;
import com.std.dga.governance.bean.AssessParam;
import com.std.dga.governance.bean.GovernanceAssessDetail;
import com.std.dga.meta.bean.TableMetaInfo;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

@Component("TIME_LINESS")
public class TableProduceTimeAssessor extends Assessor {

    @Autowired
    TDsTaskInstanceService tDsTaskInstanceService;

    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws Exception {

        //排除ods层的表
        if("ODS".equals(assessParam.getTableMetaInfo().getTableMetaInfoExtra().getDwLevel())){
            return ;
        }

        // 获得考评指标参数
        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        JSONObject jsonObject = JSON.parseObject(metricParamsJson);
        Integer days = jsonObject.getInteger("days");
        Integer percent = jsonObject.getInteger("percent");

        // 获取任务名称
        TableMetaInfo tableMetaInfo = assessParam.getTableMetaInfo();
        String taskName = tableMetaInfo.getSchemaName() + "." + tableMetaInfo.getTableName();

        long startTime = assessParam.getTDsTaskInstance().getStartTime().getTime();
        long endTime = assessParam.getTDsTaskInstance().getEndTime().getTime();
        // 当前时效（单位：秒）
        long currentDurationSec = (endTime - startTime) / 1000;

        // 计算前days天的平均时效
        String assessDate = assessParam.getAssessDate();
        Date assessDateDt = DateUtils.parseDate(assessDate, "yyyy-MM-dd");
        Date startDateDt = DateUtils.addDays(assessDateDt, -days);
        String startDate = DateFormatUtils.format(startDateDt, "yyyy-MM-dd");

        List<TDsTaskInstance> tDsTaskInstanceList
                = tDsTaskInstanceService.selectBeforeNDaysInstance(taskName, startDate, assessDate);
        if (tDsTaskInstanceList.size() > 0){
            // 求前days总时效
            long totalDurationSec = 0L;
            for (TDsTaskInstance tDsTaskInstance : tDsTaskInstanceList) {
                long beforeDurationSec =
                        (tDsTaskInstance.getEndTime().getTime() - tDsTaskInstance.getStartTime().getTime()) / 1000;
                totalDurationSec += beforeDurationSec;
            }
            // 求平均时效
            long avgDurationSec = totalDurationSec / tDsTaskInstanceList.size();
            if(currentDurationSec > (avgDurationSec +  avgDurationSec  * percent / 100)){
                governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
                governanceAssessDetail.setAssessProblem("当日产出时效超过前" + days + "天平均产出时效的" + percent + "%");
                governanceAssessDetail.setAssessComment("当日产出时效: " +  currentDurationSec + " , 前"+ tDsTaskInstanceList.size() +"天的平均产出时效: " + avgDurationSec);
            }
        }



    }
}
