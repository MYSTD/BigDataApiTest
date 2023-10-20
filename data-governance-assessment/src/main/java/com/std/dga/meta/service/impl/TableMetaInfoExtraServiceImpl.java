package com.std.dga.meta.service.impl;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.std.dga.meta.constant.MetaConst;
import com.std.dga.meta.bean.TableMetaInfo;
import com.std.dga.meta.bean.TableMetaInfoExtra;
import com.std.dga.meta.mapper.TableMetaInfoExtraMapper;
import com.std.dga.meta.service.TableMetaInfoExtraService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * <p>
 * 元数据表附加信息 服务实现类
 * </p>
 *
 * @author std
 * @since 2023-10-07
 */
@Service
@DS("dga")
public class TableMetaInfoExtraServiceImpl extends ServiceImpl<TableMetaInfoExtraMapper, TableMetaInfoExtra> implements TableMetaInfoExtraService {

    /**
     * 初始化表辅助信息
     */

    public void initTableMetaExtra(List<TableMetaInfo> tableMetaInfoList) {
        List<TableMetaInfoExtra> tableMetaInfoExtraList = new ArrayList<>(tableMetaInfoList.size());

        for (TableMetaInfo tableMetaInfo : tableMetaInfoList) {
            // 先查询表辅助信息是否存在
            TableMetaInfoExtra tableMetaInfoExtra = getOne(
                    new QueryWrapper<TableMetaInfoExtra>()
                            .eq("schema_name", tableMetaInfo.getSchemaName())
                            .eq("table_name", tableMetaInfo.getTableName()));
            // 如果不存在，则进行初始化
            if( tableMetaInfoExtra == null ){
                tableMetaInfoExtra = new TableMetaInfoExtra() ;
                tableMetaInfoExtra.setSchemaName(tableMetaInfo.getSchemaName());
                tableMetaInfoExtra.setTableName(tableMetaInfo.getTableName());
                tableMetaInfoExtra.setSecurityLevel(MetaConst.SECURITY_LEVEL_UNSET);
                tableMetaInfoExtra.setLifecycleType(MetaConst.LIFECYCLE_TYPE_UNSET);

                //配置所有者，默认为未设置
                tableMetaInfoExtra.setBusiOwnerUserName(MetaConst.BUSI_OWNER_USER_NAME_UNSET);
                tableMetaInfoExtra.setTecOwnerUserName(MetaConst.TEC_OWNER_USER_NAME_UNSET);

                tableMetaInfoExtra.setLifecycleDays(-1L);
                tableMetaInfoExtra.setDwLevel(getTableLevelByName(tableMetaInfoExtra.getTableName()));

                tableMetaInfoExtra.setCreateTime(new Date());
                //保存到集合中，做批写
                tableMetaInfoExtraList.add(tableMetaInfoExtra) ;
            }

//            remove(new QueryWrapper<TableMetaInfoExtra>().eq("schema_name" , "gmall"));
            //存储
            saveOrUpdateBatch(tableMetaInfoExtraList);
        }
    }

    private String getTableLevelByName(String tableName) {
        if(tableName.startsWith("ods")){
            return "ODS";
        } else if (tableName.startsWith("dwd")) {
            return "DWD";
        }else if (tableName.startsWith("dim")) {
            return "DIM";
        }else if (tableName.startsWith("dws")) {
            return "DWS";
        }else if (tableName.startsWith("ads")) {
            return "ADS";
        }else if (tableName.startsWith("dm")) {
            return "DM";
        }else  {
            return "OTHER";
        }

    }

}
