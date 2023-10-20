package com.std.dga.assessor.spec;

import com.std.dga.assessor.Assessor;
import com.std.dga.governance.bean.AssessParam;
import com.std.dga.governance.bean.GovernanceAssessDetail;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component("TABLE_NAME_STANDARD")
public class TableNameStandardAssessor extends Assessor {

    Pattern odsPattern = Pattern.compile("^ods_\\w+_(inc|full)$");
    Pattern dimPattern = Pattern.compile("^dim_\\w+(_(zip|full)$)?");
    Pattern dwdPattern = Pattern.compile("^dwd_\\w+_\\w+_(inc|full|acc)$");
    Pattern dwsPattern = Pattern.compile("^dws_\\w+_\\w+_\\w+_(1d|nd|td)$");
    Pattern adsPattern = Pattern.compile("^ads_\\w+$");
    Pattern dmPattern = Pattern.compile("^dm_\\w+$");

    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws Exception {

        // 获取表的名字， 查看表名是否符合规范
        String tableName = assessParam.getTableMetaInfo().getTableName();
        String dwLevel = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getDwLevel();
        Matcher matcher = null ;
        switch (dwLevel) {
            case "ODS":
                matcher = odsPattern.matcher(tableName);
                break;
            case "DIM":
                matcher = dimPattern.matcher(tableName);
                break;
            case "DWD":
                matcher = dwdPattern.matcher(tableName);
                break;
            case "DWS":
                matcher = dwsPattern.matcher(tableName);
                break;
            case "ADS":
                matcher = adsPattern.matcher(tableName);
                break;
            case "DM":
                matcher = dmPattern.matcher(tableName);
                break;
            default:
                //未纳入分层
                governanceAssessDetail.setAssessScore(BigDecimal.valueOf(5L));
                governanceAssessDetail.setAssessProblem("未纳入分层");
                return ;
        }

        //处理匹配结果
        boolean matches = matcher.matches();
        if(!matches){
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("表名不符合当前层的规范");
        }


    }
}
