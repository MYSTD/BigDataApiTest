package com.std.dga.governance.bean;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import java.io.Serializable;
import java.math.BigDecimal;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

/**
 * <p>
 * 治理考评类别权重表
 * </p>
 *
 * @author std
 * @since 2023-10-10
 */
@Data
@TableName("governance_type")
public class GovernanceType implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * id
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * 治理项类型编码
     */
    private String typeCode;

    /**
     * 治理项类型描述
     */
    private String typeDesc;

    /**
     * 治理类型权重
     */
    private BigDecimal typeWeight;
}
