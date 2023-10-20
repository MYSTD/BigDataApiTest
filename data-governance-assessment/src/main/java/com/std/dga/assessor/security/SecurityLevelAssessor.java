package com.std.dga.assessor.security;

import com.std.dga.assessor.Assessor;
import com.std.dga.governance.bean.AssessParam;
import com.std.dga.governance.bean.GovernanceAssessDetail;
import com.std.dga.meta.constant.MetaConst;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component("SECURITY_LEVEL")
public class SecurityLevelAssessor extends Assessor {

    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws Exception {

        String securityLevel = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getSecurityLevel();
        if(securityLevel == null || securityLevel.trim().isEmpty() || securityLevel.equals(MetaConst.SECURITY_LEVEL_UNSET)){
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("未设置安全级别");

            //默认是 /table_meta/table_meta/detail?tableId={tableId}
            String governanceUrl = assessParam.getGovernanceMetric().getGovernanceUrl();
            governanceUrl = governanceUrl.replace("{tableId}", assessParam.getTableMetaInfo().getId() + "");
            governanceAssessDetail.setGovernanceUrl(governanceUrl);
        }

    }
}
