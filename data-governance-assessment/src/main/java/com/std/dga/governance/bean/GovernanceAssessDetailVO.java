package com.std.dga.governance.bean;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

/**
 * ClassName:GovernanceAssessDetailVO
 * Description:
 *
 * @date:2023/10/16 18:59
 * @author:STD
 */
@Data
@TableName("governance_assess_detail")
public class GovernanceAssessDetailVO {

    private static final long serialVersionUID = 1L;

    /**
     * id
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * 考评日期
     */
    private String assessDate;

    /**
     * 表名
     */
    private String tableName;

    /**
     * 库名
     */
    private String schemaName;


    /**
     * 指标项名称
     */
    private String metricName;

    /**
     * 技术负责人
     */
    private String tecOwner;


    /**
     * 考评问题项
     */
    private String assessProblem;



    /**
     * 治理处理路径
     */
    private String governanceUrl;
}
