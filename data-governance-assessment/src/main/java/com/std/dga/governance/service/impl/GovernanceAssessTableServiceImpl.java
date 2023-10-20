package com.std.dga.governance.service.impl;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.std.dga.governance.bean.GovernanceAssessTable;
import com.std.dga.governance.bean.GovernanceAssessTecOwner;
import com.std.dga.governance.bean.GovernanceType;
import com.std.dga.governance.constant.GovernanceConstant;
import com.std.dga.governance.mapper.GovernanceAssessTableMapper;
import com.std.dga.governance.service.GovernanceAssessTableService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.std.dga.governance.service.GovernanceTypeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * 表治理考评情况 服务实现类
 * </p>
 *
 * @author std
 * @since 2023-10-16
 */
@Service
@DS("dga")
public class GovernanceAssessTableServiceImpl extends ServiceImpl<GovernanceAssessTableMapper, GovernanceAssessTable> implements GovernanceAssessTableService {

    @Autowired
    GovernanceTypeService governanceTypeService;

    @Override
    public void calcGovernanceAssessTable(String assessDate) {

        remove(new QueryWrapper<GovernanceAssessTable>().eq("assess_date", assessDate));

        // 直接调用Mapper里面的方法
        List<GovernanceAssessTable> governanceAssessTables =
                getBaseMapper().selectGovernanceAssessTableByDetail(assessDate);

        List<GovernanceType> governanceTypeList = governanceTypeService.list();
        Map<String, BigDecimal> governanceTypeMap = new HashMap<>();
        for (GovernanceType governanceType : governanceTypeList) {
            governanceTypeMap.put(governanceType.getTypeCode(), governanceType.getTypeWeight());
        }

        for (GovernanceAssessTable governanceAssessTable : governanceAssessTables) {
            BigDecimal specScore =
                    governanceAssessTable.getScoreSpecAvg()
                            .multiply(governanceTypeMap.get(GovernanceConstant.GOVERNANCE_TYPE_SPEC)
                                    .divide(BigDecimal.TEN, 1, RoundingMode.HALF_UP));
            BigDecimal storageScore =
                    governanceAssessTable.getScoreStorageAvg()
                            .multiply(governanceTypeMap.get(GovernanceConstant.GOVERNANCE_TYPE_STORAGE)
                                    .divide(BigDecimal.TEN, 1, RoundingMode.HALF_UP));
            BigDecimal calcScore =
                    governanceAssessTable.getScoreCalcAvg()
                            .multiply(governanceTypeMap.get(GovernanceConstant.GOVERNANCE_TYPE_CALC)
                                    .divide(BigDecimal.TEN, 1, RoundingMode.HALF_UP));
//            BigDecimal qualityScore =
//                    governanceAssessTable.getScoreQualityAvg()
//                            .multiply(governanceTypeMap.get(GovernanceConstant.GOVERNANCE_TYPE_QUALITY)
//                                    .divide(BigDecimal.TEN, 1, RoundingMode.HALF_UP));
            BigDecimal securityScore =
                    governanceAssessTable.getScoreSecurityAvg()
                            .multiply(governanceTypeMap.get(GovernanceConstant.GOVERNANCE_TYPE_SECURITY)
                                    .divide(BigDecimal.TEN, 1, RoundingMode.HALF_UP));

            BigDecimal sumScore =
                    specScore.add(storageScore).add(calcScore).add(securityScore);//.add(qualityScore);

            governanceAssessTable.setScoreOnTypeWeight(sumScore);

        }
        //写入到数据库中
        saveBatch(governanceAssessTables);
    }
}
