package com.std.dga.assessor.storage;

import com.std.dga.assessor.Assessor;
import com.std.dga.governance.bean.AssessParam;
import com.std.dga.governance.bean.GovernanceAssessDetail;
import com.std.dga.meta.bean.TableMetaInfo;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component("TABLE_EMPTY")
public class TableEmptyAssessor extends Assessor {

    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) {

        TableMetaInfo tableMetaInfo = assessParam.getTableMetaInfo();
        if (tableMetaInfo.getTableSize() == 0L){
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("表中无数据");
        }
    }
}
