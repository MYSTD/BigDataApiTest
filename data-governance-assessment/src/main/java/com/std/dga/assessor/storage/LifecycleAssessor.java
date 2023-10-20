package com.std.dga.assessor.storage;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.std.dga.assessor.Assessor;
import com.std.dga.governance.bean.AssessParam;
import com.std.dga.governance.bean.GovernanceAssessDetail;
import com.std.dga.meta.bean.TableMetaInfo;
import com.std.dga.meta.constant.MetaConst;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component("LIFECYCLE")
public class LifecycleAssessor extends Assessor {

    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws Exception {

        // 获得考评指标参数
        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        JSONObject jsonObject = JSON.parseObject(metricParamsJson);
        Integer days = jsonObject.getInteger("days");

        TableMetaInfo tableMetaInfo = assessParam.getTableMetaInfo();

        String lifecycleType = tableMetaInfo.getTableMetaInfoExtra().getLifecycleType();
        // 判断是否设置周期类型
        if (lifecycleType == null || lifecycleType.trim().isEmpty() || lifecycleType.equals(MetaConst.LIFECYCLE_TYPE_UNSET)){
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("未设置周期类型");
            //判断是否为日分区
        } else if (lifecycleType.equals(MetaConst.LIFECYCLE_TYPE_DAY)) {
            String partitionColNameJson = tableMetaInfo.getPartitionColNameJson();
            // 判断有无分区信息
            if (partitionColNameJson == null || partitionColNameJson.trim().isEmpty()){
                governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
                governanceAssessDetail.setAssessProblem("日分区无分区信息");
                return;
            }

            Long lifecycleDays = tableMetaInfo.getTableMetaInfoExtra().getLifecycleDays();
            // 判断是否设置周期
            if (lifecycleDays == -1L){
                governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
                governanceAssessDetail.setAssessProblem("未设生命周期");
            } else if (lifecycleDays > days) {
                long scord = days * 10 / lifecycleDays ;
                governanceAssessDetail.setAssessScore(BigDecimal.valueOf(scord));
                governanceAssessDetail.setAssessProblem("周期长度超过建议周期天数: " + days + " 天");
            }
        }
    }
}
