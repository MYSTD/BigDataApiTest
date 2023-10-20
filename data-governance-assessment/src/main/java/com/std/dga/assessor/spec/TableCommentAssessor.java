package com.std.dga.assessor.spec;

import com.std.dga.assessor.Assessor;
import com.std.dga.governance.bean.AssessParam;
import com.std.dga.governance.bean.GovernanceAssessDetail;
import com.std.dga.meta.bean.TableMetaInfo;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * ClassName:TableCommentAssessor
 * Description:
 *
 * @date:2023/10/11 16:56
 * @author:STD
 */
@Component("TABLE_COMMENT")
public class TableCommentAssessor extends Assessor {
    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) {
        TableMetaInfo tableMetaInfo = assessParam.getTableMetaInfo();
        if (tableMetaInfo.getTableComment()==null&&tableMetaInfo.getTableComment().isEmpty()){
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("缺少表备注");
        }
    }
}
