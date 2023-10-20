package com.std.dga.assessor.quality;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.std.dga.assessor.Assessor;
import com.std.dga.governance.bean.AssessParam;
import com.std.dga.governance.bean.GovernanceAssessDetail;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.net.URI;
import java.util.Date;
import java.util.List;

@Component("PRODUCE_DATA_SIZE")
public class TableProduceDataSizeAssessor extends Assessor {

    @Value("${hdfs.uri}")
    private String hdfsUri;
    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws Exception {

        // 获得考评指标参数
        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        JSONObject jsonObject = JSON.parseObject(metricParamsJson);
        Integer days = jsonObject.getInteger("days");
        Integer upperLimit = jsonObject.getInteger("upper_limit");
        Integer lowerLimit = jsonObject.getInteger("lower_limit");

        // 判断是否是日分区
//        if (assessParam.getTableMetaInfo().getTableMetaInfoExtra().getLifecycleType().equals(MetaConst.LIFECYCLE_TYPE_DAY)){
//
//        }

        // TODO 分区路径: /warehouse/gmall/ods/ods_log_inc/dt=2023-05-01
        // 获取表路径
        String tableFsPath = assessParam.getTableMetaInfo().getTableFsPath();
        
        // 获取分区名称（考虑可能有二级分区）
        String partitionColNameJson = assessParam.getTableMetaInfo().getPartitionColNameJson();
        if (partitionColNameJson == null || partitionColNameJson.trim().isEmpty())
            return;
        List<JSONObject> jsonObjects = JSON.parseArray(partitionColNameJson, JSONObject.class);
        if (jsonObjects.size() == 0)
            return;
        String partitionName = jsonObjects.get(0).getString("name");
        
        // 获取当日日期
        String assessDate = assessParam.getAssessDate();
        Date assessDateDt = DateUtils.parseDate(assessDate, "yyyy-MM-dd");
        // 当前日期为考评日期的前一天
        Date currentDateDt = DateUtils.addDays(assessDateDt, -1);
        String currentDateString = DateFormatUtils.format(currentDateDt, "yyyy-MM-dd");
        // 拼接分区路径
        String currentPartitionPath = tableFsPath + "/" + partitionName + "=" + currentDateString;

        // 获取用户
        String tableFsOwner = assessParam.getTableMetaInfo().getTableFsOwner();

        // 获取当日数据大小
        Long currentDataSize = getDateSize(currentPartitionPath, tableFsOwner);

        Long daySum = 0L;
        Long beforeDateSizeSum = 0L;
        for (int i = 1; i <= days; i++) {
            Date beforeDateDt = DateUtils.addDays(currentDateDt, -i);
            String beforeDateString = DateFormatUtils.format(beforeDateDt, "yyyy-MM-dd");
            String beforePartitionPath = tableFsPath + "/" + partitionName + "=" + beforeDateString;
            Long beforeDateSize = getDateSize(beforePartitionPath, tableFsOwner);
            if (beforeDateSize > 0){
                // 存在该分区
                daySum++;
                beforeDateSizeSum += beforeDateSize;
            }
        }


        if(beforeDateSizeSum > 0 ) {
            // 计算前days天的平均产出数据量
            long avgDataSize = beforeDateSizeSum / daySum;
            Double doubleUpperDataSize = avgDataSize * ( 1 + (double)upperLimit / 100 );
            long upperDataSize = doubleUpperDataSize.longValue();
            long lowerDataSize = avgDataSize * lowerLimit / 100 ;

            if( currentDataSize > upperDataSize || currentDataSize < lowerDataSize  ){
                governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
                governanceAssessDetail.setAssessProblem("当天数据量超出前 " + daySum +" 天平均产出数据量的"+ upperLimit +"%, 或者低于" + lowerLimit+"%");
            }
            governanceAssessDetail.setAssessComment("当天数据量: " + currentDataSize + " , 前" + daySum +"天的平均数据量: " + avgDataSize  );
        }


    }

    /**
     * 获取分区数据大小
     */
    public Long getDateSize(String fsPath,String fsOwner) throws Exception {
        FileSystem fs = FileSystem.get(new URI(hdfsUri), new Configuration(), fsOwner);
        // 判断路径是否存在
        if(!fs.exists(new Path(fsPath)))
            return 0L;
        FileStatus[] fileStatuses = fs.listStatus(new Path(fsPath));

        return sumDateSize(fs,fileStatuses,0L);
    }

    /**
     * 递归获取分区数据大小
     */
    public Long sumDateSize(FileSystem fs,FileStatus[] fileStatuses,Long dateSize) throws Exception {
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()){
                dateSize += fileStatus.getLen();
            }else {
                // 获取目录下内容
                FileStatus[] downFileStatuses = fs.listStatus(fileStatus.getPath());
                dateSize = sumDateSize(fs,downFileStatuses,dateSize);
            }
        }
        return dateSize;
    }
}


