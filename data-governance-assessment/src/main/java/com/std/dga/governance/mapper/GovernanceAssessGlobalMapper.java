package com.std.dga.governance.mapper;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.std.dga.governance.bean.GovernanceAssessGlobal;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * <p>
 * 治理总考评表 Mapper 接口
 * </p>
 *
 * @author std
 * @since 2023-10-16
 */
@Mapper
@DS("dga")
public interface GovernanceAssessGlobalMapper extends BaseMapper<GovernanceAssessGlobal> {

    @Select("SELECT \n" +
            "    assess_date ,\n" +
            "    AVG(score_spec_avg * 10) score_spec ,\n" +
            "    AVG(score_storage_avg * 10) score_storage  ,\n" +
            "    AVG(score_calc_avg * 10) score_calc ,\n" +
            "    AVG(score_quality_avg * 10) score_quality ,\n" +
            "    AVG(score_security_avg * 10) score_security , \n" +
            "    AVG(score_on_type_weight) score, \n" +
            "    COUNT(*) table_num,\n" +
            "    SUM(problem_num) problem_num,\n" +
            "    NOW() create_time \n" +
            "    \n" +
            "FROM governance_assess_table \n" +
            "WHERE assess_date = #{assessDate} \n" +
            "GROUP BY assess_date\n"
    )
    List<GovernanceAssessGlobal> getAssessGlobal(String assessDate);

}
