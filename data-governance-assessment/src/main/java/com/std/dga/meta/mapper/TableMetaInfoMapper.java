package com.std.dga.meta.mapper;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.std.dga.meta.bean.TableMetaInfo;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.std.dga.meta.bean.TableMetaInfoVO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * <p>
 * 元数据表 Mapper 接口
 * </p>
 *
 * @author std
 * @since 2023-10-07
 */
@Mapper
@DS("dga")
public interface TableMetaInfoMapper extends BaseMapper<TableMetaInfo> {

    @Select(
            "SELECT ti.*, te.* , ti.id ti_id , te.id te_id\n" +
                    "FROM table_meta_info ti JOIN table_meta_info_extra te\n" +
                    "ON ti.schema_name = te.schema_name  AND ti.table_name = te.table_name  \n" +
                    "WHERE ti.assess_date = #{assessDate} "
    )
    @ResultMap("meta_result_map")
    List<TableMetaInfo> selectTableMetaInfoList(String assessDate);

    @Select("${sql}")
    List<TableMetaInfoVO> selectTableMetaInfoVoList(String sql);
    @Select("${sql}")
    Integer selectTableMetaInfoCount(String sql);
}
