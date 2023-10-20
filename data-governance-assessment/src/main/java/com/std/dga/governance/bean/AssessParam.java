package com.std.dga.governance.bean;

import com.std.dga.dolphinscheduler.bean.TDsTaskDefinition;
import com.std.dga.dolphinscheduler.bean.TDsTaskInstance;
import com.std.dga.meta.bean.TableMetaInfo;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * ClassName:AssessParam
 * Description:
 *
 * @date:2023/10/10 16:31
 * @author:STD
 */
@Data
public class AssessParam {

    private String assessDate ;
    private TableMetaInfo tableMetaInfo ;
    private GovernanceMetric governanceMetric ;

    private Map<String ,TableMetaInfo> tableMetaInfoMap ; // 所有表

    private TDsTaskDefinition tDsTaskDefinition ; // 该指标需要的任务定义

    private TDsTaskInstance tDsTaskInstance ; // 该指标需要的任务状态信息(当日)

}
