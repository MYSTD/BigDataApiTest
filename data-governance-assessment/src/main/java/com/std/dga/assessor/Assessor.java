package com.std.dga.assessor;

import com.std.dga.governance.bean.AssessParam;
import com.std.dga.governance.bean.GovernanceAssessDetail;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.Date;

/**
 * ClassName:Assessor
 * Description: 考评器父类
 * @date:2023/10/10 16:28
 * @author:STD
 */
public abstract class Assessor {

    public final GovernanceAssessDetail doAssessor(AssessParam assessParam){

//        System.out.println("Assessor 管理流程");
        GovernanceAssessDetail governanceAssessDetail = new GovernanceAssessDetail();
        governanceAssessDetail.setAssessDate(assessParam.getAssessDate());
        governanceAssessDetail.setMetricId(assessParam.getTableMetaInfo().getId()+"");
        governanceAssessDetail.setTableName(assessParam.getTableMetaInfo().getTableName());
        governanceAssessDetail.setSchemaName(assessParam.getTableMetaInfo().getSchemaName());
        governanceAssessDetail.setMetricName(assessParam.getGovernanceMetric().getMetricName());
        governanceAssessDetail.setGovernanceType(assessParam.getGovernanceMetric().getGovernanceType());
        governanceAssessDetail.setTecOwner(assessParam.getTableMetaInfo().getTableMetaInfoExtra().getTecOwnerUserName());
        governanceAssessDetail.setCreateTime(new Date());
        //默认先给满分， 在考评器查找问题的过程中，如果有问题，再按照指标的要求重新给分。
        governanceAssessDetail.setAssessScore(BigDecimal.TEN);

        try {
            checkProblem(governanceAssessDetail, assessParam);
        }catch (Exception e) {
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setIsAssessException("1");
            //记录异常信息
            //简单记录
            //governanceAssessDetail.setAssessExceptionMsg( e.getMessage());

            //详细记录
            StringWriter stringWriter = new StringWriter() ;
            PrintWriter msgPrintWriter = new PrintWriter(stringWriter) ;
            e.printStackTrace( msgPrintWriter);
            governanceAssessDetail.setAssessExceptionMsg( stringWriter.toString().substring( 0,  Math.min( 2000 , stringWriter.toString().length())) );
        }

        return governanceAssessDetail;
    }

    public abstract void checkProblem(GovernanceAssessDetail governanceAssessDetail , AssessParam assessParam) throws Exception;
}
