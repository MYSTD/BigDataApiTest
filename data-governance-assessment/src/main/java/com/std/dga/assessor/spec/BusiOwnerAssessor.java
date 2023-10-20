package com.std.dga.assessor.spec;

import com.std.dga.assessor.Assessor;
import com.std.dga.governance.bean.AssessParam;
import com.std.dga.governance.bean.GovernanceAssessDetail;
import com.std.dga.meta.constant.MetaConst;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * ClassName:BusiOwnerAssessor
 * Description:
 *
 * @date:2023/10/10 18:37
 * @author:STD
 */
@Component(value = "BUSI_OWNER")
public class BusiOwnerAssessor extends Assessor {
    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) {
        String busiOwnerUserName = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getBusiOwnerUserName();

        if (busiOwnerUserName == null || busiOwnerUserName.equals(MetaConst.TEC_OWNER_USER_NAME_UNSET)) {

            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("未设置业务负责人");

            //默认是 /table_meta/table_meta/detail?tableId={tableId}
            String governanceUrl = assessParam.getGovernanceMetric().getGovernanceUrl();

            governanceUrl = governanceUrl.replace("{tableId}", assessParam.getTableMetaInfo().getId() + "");
            governanceAssessDetail.setGovernanceUrl(governanceUrl);
        }
    }
}
