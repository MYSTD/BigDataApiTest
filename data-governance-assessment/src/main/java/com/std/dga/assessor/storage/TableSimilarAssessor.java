package com.std.dga.assessor.storage;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.std.dga.assessor.Assessor;
import com.std.dga.governance.bean.AssessParam;
import com.std.dga.governance.bean.GovernanceAssessDetail;
import com.std.dga.meta.bean.TableMetaInfo;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component("TABLE_SIMILAR")
public class TableSimilarAssessor extends Assessor {

    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws Exception {

        // 获得考评指标参数
        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        JSONObject jsonObject = JSON.parseObject(metricParamsJson);
        Integer percent = jsonObject.getInteger("percent");
        //当前表
        TableMetaInfo tableMetaInfo = assessParam.getTableMetaInfo();
        //取出所有的表
        Collection<TableMetaInfo> tableMetaInfoList = assessParam.getTableMetaInfoMap().values() ;

        //相似表集合
        List<String> similarList = new ArrayList<>();
        for (TableMetaInfo otherTable : tableMetaInfoList) {
            // 同表
            if (otherTable.getTableName().equals(tableMetaInfo.getTableName())){
                continue;
            }
            // 层次不相同的表
            if (!otherTable.getTableMetaInfoExtra().getDwLevel().equals(tableMetaInfo.getTableMetaInfoExtra().getDwLevel())){
                continue;
            }

            // 提取当前表与另一表的字段名称
            List<String> currentTableColList = extractTableColName(tableMetaInfo);
            List<String> otherTableColList = extractTableColName(otherTable);

            // 取交集
            List<String> intersection = new ArrayList<>(currentTableColList);
            intersection.retainAll(otherTableColList);
//            //取交集
//            Collection intersection = CollectionUtils.intersection(currentTableColList, otherTableColList);

            // 判断是否为相似表
            if (intersection.size() > 0){
                if (intersection.size() * 100 / currentTableColList.size() > percent){
                    similarList.add(otherTable.getTableName());
                }
            }

            if (similarList.size() > 0){
                governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
                governanceAssessDetail.setAssessProblem("同层次存在相似表");
                governanceAssessDetail.setAssessComment("相似表为: " + similarList );

            }

        }

    }

    /**
     * 提取字段名称列表
     */
    private List<String> extractTableColName(TableMetaInfo tableMetaInfo) {
        String colNameJson = tableMetaInfo.getColNameJson();
        List<JSONObject> jsonObjects = JSON.parseArray(colNameJson, JSONObject.class);

        List<String> tableColNameList = jsonObjects.stream()
                .map(jsonObject -> jsonObject.getString("name")) // 将字段属性替换成字段名称
                .collect(Collectors.toList()); // 以转为list对象（采集）

        return tableColNameList;
    }
}
