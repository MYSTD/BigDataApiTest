package com.std.dga.governance.service.impl;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.std.dga.governance.bean.GovernanceAssessTecOwner;
import com.std.dga.governance.mapper.GovernanceAssessTecOwnerMapper;
import com.std.dga.governance.service.GovernanceAssessTecOwnerService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * 技术负责人治理考评表 服务实现类
 * </p>
 *
 * @author std
 * @since 2023-10-16
 */
@Service
@DS("dga")
public class GovernanceAssessTecOwnerServiceImpl extends ServiceImpl<GovernanceAssessTecOwnerMapper, GovernanceAssessTecOwner> implements GovernanceAssessTecOwnerService {


    @Override
    public void calcAssessTecOwner(String assessDate) {
        remove(new QueryWrapper<GovernanceAssessTecOwner>().eq("assess_date", assessDate));

        List<GovernanceAssessTecOwner> assessTecOwnerList = baseMapper.getAssessTecOwner(assessDate);
        saveBatch(assessTecOwnerList);

    }

    /**
     * 返回结果：
     * [{"tecOwner":"zhang3" ,"score":99},
     * {"tecOwner":"li4" ,"score":98},
     * {"tecOwner": "wang5","score":97}]
     *
     * listMaps 返回的是一个List<Map<String, Object>>集合，
     * list集合中的每个map集合代表一条数据，k为字段名，v为字段值
     */
    @Override
    public List<Map<String, Object>> getRankList() {
        List<Map<String, Object>> resultList = listMaps(new QueryWrapper<GovernanceAssessTecOwner>()
                .select("tec_owner as tecOwner", "score")
                .inSql("assess_date", "select max(assess_date) from governance_assess_tec_owner")
                .orderByDesc("score"));
        return resultList;
    }
}
