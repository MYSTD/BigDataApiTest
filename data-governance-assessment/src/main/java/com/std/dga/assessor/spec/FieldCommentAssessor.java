package com.std.dga.assessor.spec;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.std.dga.assessor.Assessor;
import com.std.dga.governance.bean.AssessParam;
import com.std.dga.governance.bean.GovernanceAssessDetail;
import com.std.dga.meta.bean.TableMetaInfo;
import lombok.var;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Component("FIELD_COMMENT")
public class FieldCommentAssessor extends Assessor {

    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) {

        TableMetaInfo tableMetaInfo = assessParam.getTableMetaInfo();
        String colNameJson = tableMetaInfo.getColNameJson();

        if (colNameJson != null && !colNameJson.trim().isEmpty()) {
            List<JSONObject> colNameJsonList = JSON.parseArray(colNameJson, JSONObject.class);
            // 存储缺少备注的字段名
            Set<String> ColCommentMissSet = new HashSet<>();
            for (JSONObject jsonObject : colNameJsonList) {
                String comment = jsonObject.getString("comment");
                if(comment==null||comment.isEmpty()){
                    ColCommentMissSet.add(jsonObject.getString("name"));
                }
            }
            if (ColCommentMissSet.size() > 0) {
                Long score = (colNameJsonList.size() - ColCommentMissSet.size()) * 10L / colNameJsonList.size();
                governanceAssessDetail.setAssessScore(BigDecimal.valueOf(score));
                governanceAssessDetail.setAssessProblem("缺少字段备注" );
                governanceAssessDetail.setAssessComment("缺少备注的字段为：" + JSON.toJSONString(ColCommentMissSet));
            }
        }
    }
}
