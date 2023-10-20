package com.std.dga.governance.service.impl;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.std.dga.governance.bean.GovernanceAssessGlobal;
import com.std.dga.governance.mapper.GovernanceAssessGlobalMapper;
import com.std.dga.governance.service.GovernanceAssessGlobalService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * 治理总考评表 服务实现类
 * </p>
 *
 * @author std
 * @since 2023-10-16
 */
@Service
@DS("dga")
public class GovernanceAssessGlobalServiceImpl extends ServiceImpl<GovernanceAssessGlobalMapper, GovernanceAssessGlobal> implements GovernanceAssessGlobalService {

    @Override
    public void calcAssessGlobal(String assessDate) {

        remove(new QueryWrapper<GovernanceAssessGlobal>().eq("assess_date" , assessDate));

        List<GovernanceAssessGlobal> assessGlobal = baseMapper.getAssessGlobal(assessDate);
        saveBatch(assessGlobal);

    }

    /**
     * 获取最新的全局分数
     * 
     * 返回结果：{"assessDate":"2023-04-01" ,"sumScore":90, "scoreList":[20,40,34,55,66]}
     */
    @Override
    public Map<String, Object> getLastGlobalScore() {
        GovernanceAssessGlobal governanceAssessGlobal = getOne(new QueryWrapper<GovernanceAssessGlobal>()
                .orderByDesc("assess_date")
                .last("limit 1 "));
        // 封装各类考评分数
        ArrayList<BigDecimal> scoreList = new ArrayList<>();
        scoreList.add(governanceAssessGlobal.getScoreSpec());
        scoreList.add(governanceAssessGlobal.getScoreStorage());
        scoreList.add(governanceAssessGlobal.getScoreCalc());
        scoreList.add(governanceAssessGlobal.getScoreQuality());
        scoreList.add(governanceAssessGlobal.getScoreSecurity());

        // 封装返回值
        HashMap<String, Object> resultMap = new HashMap<>();
        resultMap.put("assessDate",governanceAssessGlobal.getAssessDate());
        resultMap.put("sumScore",governanceAssessGlobal.getScore());
        resultMap.put("scoreList",scoreList);

        return resultMap;
    }
}
