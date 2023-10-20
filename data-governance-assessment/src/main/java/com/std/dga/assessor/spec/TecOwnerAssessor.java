package com.std.dga.assessor.spec;

import com.std.dga.assessor.Assessor;
import com.std.dga.governance.bean.AssessParam;
import com.std.dga.governance.bean.GovernanceAssessDetail;
import com.std.dga.meta.constant.MetaConst;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * ClassName:TecOwnerAssessor
 * Description:
 *
 * @date:2023/10/10 16:32
 * @author:STD
 */
@Component(value = "TEC_OWNER")
public class TecOwnerAssessor extends Assessor {

    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) {

        String tecOwnerUserName = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getTecOwnerUserName();

        if(tecOwnerUserName==null||tecOwnerUserName.equals(MetaConst.TEC_OWNER_USER_NAME_UNSET)){

            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("未设置技术负责人");

            //默认是 /table_meta/table_meta/detail?tableId={tableId}
            String governanceUrl = assessParam.getGovernanceMetric().getGovernanceUrl();

            governanceUrl = governanceUrl.replace("{tableId}", assessParam.getTableMetaInfo().getId() + "");
            governanceAssessDetail.setGovernanceUrl(governanceUrl);
        }
    }
}
