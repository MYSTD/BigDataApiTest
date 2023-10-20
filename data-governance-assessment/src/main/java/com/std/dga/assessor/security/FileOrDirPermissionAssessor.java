package com.std.dga.assessor.security;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.std.dga.assessor.Assessor;
import com.std.dga.governance.bean.AssessParam;
import com.std.dga.governance.bean.GovernanceAssessDetail;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;

@Component("FILE_ACCESS_PERMISSION")
public class FileOrDirPermissionAssessor extends Assessor {

    @Value("${hdfs.uri}")
    private String hdfsUri;
    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws Exception {

        // 获得考评指标参数
        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        JSONObject jsonObject = JSON.parseObject(metricParamsJson);
        String filePermission = jsonObject.getString("file_permission");
        String dirPermission = jsonObject.getString("dir_permission");

        String tableFsPath = assessParam.getTableMetaInfo().getTableFsPath();
        String tableFsOwner = assessParam.getTableMetaInfo().getTableFsOwner();
        FileSystem fs = FileSystem.get(new URI(hdfsUri), new Configuration(), tableFsOwner);
        FileStatus[] fileStatuses = fs.listStatus(new Path(tableFsPath));

        Boolean isBeyond = checkFileOrDirPermission(fs, fileStatuses, filePermission, dirPermission);
        if (isBeyond){
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("目录文件数据访问权限超过建议值");
        }
    }

    public Boolean checkFileOrDirPermission(FileSystem fs,FileStatus[] fileStatuses,String filePermission,String dirPermission) throws IOException {
        for (FileStatus fileStatus : fileStatuses) {
            FsPermission permission = fileStatus.getPermission();
            if (fileStatus.isFile()){
                // 检查文件
                Boolean isBeyond = checkPermission(permission, filePermission);
                if (isBeyond) return isBeyond;
            }else {
                // 检查目录
                Boolean isBeyond = checkPermission(permission,dirPermission);
                if (isBeyond) return isBeyond;
                // 递归检查文件
                FileStatus[] downFileStatuses = fs.listStatus(fileStatus.getPath());
                isBeyond = checkFileOrDirPermission(fs, downFileStatuses, filePermission, dirPermission);
                if (isBeyond) return isBeyond;
            }
        }
        return false;
    }

    public Boolean checkPermission(FsPermission permission, String fileOrDirPermission){
        //将建议权限拆解为  user  group  other
        Integer userRWX =  Integer.parseInt(fileOrDirPermission.charAt(0)+ "") ;
        Integer groupRWX =  Integer.parseInt(fileOrDirPermission.charAt(1)+ "") ;
        Integer otherRWX =  Integer.parseInt(fileOrDirPermission.charAt(2)+ "") ;

        //判断是否越权
        if( permission.getUserAction().ordinal() > userRWX ){
            // userRWX 越权
            return true ;
        }else if (permission.getGroupAction().ordinal() > groupRWX) {
            // groupRWX 越权
            return true ;
        }else if (permission.getOtherAction().ordinal() > otherRWX){
            // otherRWX 越权
            return true ;
        }

        return false ;
    }

}
