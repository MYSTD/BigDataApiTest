package com.std.dga.assessor.calc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.std.dga.assessor.Assessor;
import com.std.dga.governance.bean.AssessParam;
import com.std.dga.governance.bean.GovernanceAssessDetail;
import com.std.dga.util.HttpUtil;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Component("DATA_SKEW")
public class DataSkewAssessor extends Assessor {

    @Value("${spark.history.url}")
    private String sparkHistoryUrlPrefix;

    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws Exception {

        // 检查当前表对应的 Spark作业中的每个Stage中的每个task的耗时
        // 表 -> SQL -> DS -> Hive -> Yarn -> yarnId
        // SparkHistory -> yarnId -> 得到相关的信息

        // 排除ODS层的表
        if (assessParam.getTableMetaInfo().getTableMetaInfoExtra().getDwLevel().equals("ODS")) {
            return;
        }
        //排除没有SQL的表
        if (assessParam.getTDsTaskDefinition() == null || assessParam.getTDsTaskDefinition().getTaskSql() == null) {
            return;
        }
        //判断是否有yarnId
        if (assessParam.getTDsTaskInstance().getAppLink() == null || assessParam.getTDsTaskInstance().getAppLink().trim().isEmpty()) {
            governanceAssessDetail.setAssessComment("没有yarnId");
            return;
        }

        // 获得考评指标参数
        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        JSONObject jsonObject = JSON.parseObject(metricParamsJson, JSONObject.class);
        Integer percent = jsonObject.getInteger("percent");
        Integer stage_dur_seconds = jsonObject.getInteger("stage_dur_seconds");

        // 获取yarnId
        String yarnId = assessParam.getTDsTaskInstance().getAppLink();

        // 获取执行成功的attemptId
        String completedAttemptId = getCompletedAttemptId(yarnId);

        // 获取stageId列表
        List<Integer> stageIdList = getStageIdList(yarnId, completedAttemptId);

        // 获取封装的Stage集合
        List<Stage> stageList = getStageList(yarnId, completedAttemptId, stageIdList);

        //判断每个stage
        for (Stage stage : stageList) {
            //判断整个stage的耗时是否超过参数值
            if(stage.getMaxTaskDuration() > stage_dur_seconds){
                // 判断每个stage中最大任务耗时是否超过参数值
                if(stage.getMaxDurationPercent() > percent ) {
                    governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
                    governanceAssessDetail.setAssessProblem("存在数据倾斜");
                }
            }
        }
        governanceAssessDetail.setAssessComment("StageList: " + stageList );
    }

    // 获取封装好的Stage对象集合
    public List<Stage> getStageList(String yarnId,String attemptId,List<Integer> stageIdList){
        // 接口地址：http://hadoop102:18080/api/v1/applications/application_1685070947994_0004/1/stages/1
        List<Stage> stageList = new ArrayList<>();
        for (Integer stageId : stageIdList) {
            String url = sparkHistoryUrlPrefix + yarnId + "/" + attemptId + "/stages/" + stageId;
            String responseBodyJsonString = HttpUtil.get(url);
            List<JSONObject> stageAttemptList = JSON.parseArray(responseBodyJsonString, JSONObject.class);
            // 遍历每次尝试
            for (JSONObject stageJsonObj : stageAttemptList) {
                if (stageJsonObj.getString("status").equals("COMPLETE")) {
                    ArrayList<Long> taskDurationList = new ArrayList<>();

                    //提取所有的task
                    JSONObject allTaskJsonObj = stageJsonObj.getJSONObject("tasks");
                    //提取所有的Key
                    Set<String> allKeys = allTaskJsonObj.keySet();
                    for (String taskKey : allKeys) {
                        JSONObject taskJsonObj = allTaskJsonObj.getJSONObject(taskKey);
                        if (taskJsonObj.getString("status").equals("SUCCESS")) {
                            //提取当前task的duration , 存入到集合中
                            taskDurationList.add(taskJsonObj.getLong("duration"));
                        }
                    }
                    // 封装Stage对象
                    Stage stage = getStage(taskDurationList);
                    stageList.add(stage);

                }

            }

        }
        return stageList;
    }

    // 封装Stage对象
    public Stage getStage(List<Long> durationList){
        Stage stage = new Stage();
        long durationMax = durationList.stream().max(Long::compareTo).get();
        long durationSum = durationList.stream().mapToLong(Long::longValue).sum();
        stage.setMaxTaskDuration(durationMax);
        stage.setStageDuration(durationSum);

        // 只有一个task
        if (durationList.size() == 1){
            stage.setAvgDuration(durationMax);
            stage.setMaxDurationPercent(0L);
        }else {
            long durationAvg = (durationSum - durationMax)/(durationList.size() - 1);
            long maxDurationPercent = (durationMax - durationAvg) * 100 / durationAvg;
            stage.setAvgDuration(durationAvg);
            stage.setMaxDurationPercent(maxDurationPercent);
        }


        return stage;
    }

    // 获取执行成功的stageId集合
    public List<Integer> getStageIdList(String yarnId, String attemptId) {
        // 接口地址：http://hadoop102:18080/api/v1/applications/application_1685070947994_0004/1/stages
        String url = sparkHistoryUrlPrefix + yarnId + "/" + attemptId + "/stages";
        String responseBodyJsonString = HttpUtil.get(url);
        List<JSONObject> stageJsonObjList = JSON.parseArray(responseBodyJsonString, JSONObject.class);
        // 获取stageId
        List<Integer> stageIdList = new ArrayList<>();
        for (JSONObject stageJsonObj : stageJsonObjList) {
            if (stageJsonObj.getString("status").equals("COMPLETE")){
                Integer stageId = stageJsonObj.getInteger("stageId");
                stageIdList.add(stageId);
            }
        }
        return stageIdList;
    }

    // 获取执行成功的attemptId(尝试Id)
    public String getCompletedAttemptId(String yarnId) {
        // 接口地址：http://hadoop102:18080/api/v1/applications/application_1685070947994_0004
        String url = sparkHistoryUrlPrefix + yarnId;
        String responseBodyJsonString = HttpUtil.get(url);
        JSONObject responseBodyJsonObj = JSON.parseObject(responseBodyJsonString);
        JSONArray attempts = responseBodyJsonObj.getJSONArray("attempts");
        for (int i = 0; i < attempts.size(); i++) {
            JSONObject attempt = attempts.getJSONObject(i);
            if (attempt.getBoolean("completed")) {
                String attemptId = attempt.getString("attemptId");
                return attemptId;
            }
        }
        return null;
    }

    @Data
    public class Stage {
        private Integer stageId;
        private Long maxTaskDuration;
        private Long stageDuration;
        private Long avgDuration;
        private Long maxDurationPercent;
    }
}
