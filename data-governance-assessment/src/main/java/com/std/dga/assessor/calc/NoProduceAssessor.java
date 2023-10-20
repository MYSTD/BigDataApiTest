package com.std.dga.assessor.calc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.std.dga.assessor.Assessor;
import com.std.dga.governance.bean.AssessParam;
import com.std.dga.governance.bean.GovernanceAssessDetail;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

@Component("NO_PRODUCE")
public class NoProduceAssessor extends Assessor {

    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws ParseException {

        // 获得考评指标参数
        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        JSONObject jsonObject = JSON.parseObject(metricParamsJson, JSONObject.class);
        Integer days = jsonObject.getInteger("days");

        // 获取表的最后访问时间
        Date tableLastModifyTime = assessParam.getTableMetaInfo().getTableLastModifyTime();
        // 截断到天
        Date tableLastAccessDate = DateUtils.truncate(tableLastModifyTime, Calendar.DATE);

        // 获取考评时间
        String assessDateString = assessParam.getAssessDate();
        Date assessDate = DateUtils.parseDate(assessDateString,"yyyy-MM-dd");

        // 求差值（单位为毫秒）
        long diffMils = Math.abs(assessDate.getTime() - tableLastAccessDate.getTime());
        // 转为天
        long diffDay = TimeUnit.DAYS.convert(diffMils, TimeUnit.MILLISECONDS);

        if(diffDay > days){
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("长期未产出");
            governanceAssessDetail.setAssessComment("超过 " + diffDay + " 天未产出");
        }

    }
}
