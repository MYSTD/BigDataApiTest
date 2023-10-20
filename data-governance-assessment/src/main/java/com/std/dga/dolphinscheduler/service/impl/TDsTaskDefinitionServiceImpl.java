package com.std.dga.dolphinscheduler.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.std.dga.dolphinscheduler.bean.TDsTaskDefinition;
import com.std.dga.dolphinscheduler.mapper.TDsTaskDefinitionMapper;
import com.std.dga.dolphinscheduler.service.TDsTaskDefinitionService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author std
 * @since 2023-10-13
 */
@Service
public class TDsTaskDefinitionServiceImpl extends ServiceImpl<TDsTaskDefinitionMapper, TDsTaskDefinition> implements TDsTaskDefinitionService {

    @Override
    public List<TDsTaskDefinition> selectList() {
        List<TDsTaskDefinition> tDsTaskDefinitionList = list();

        // 从任务task_params中将Sql单独提取出来
        for (TDsTaskDefinition tDsTaskDefinition : tDsTaskDefinitionList) {
            extractSql(tDsTaskDefinition) ;
        }

        return tDsTaskDefinitionList;
    }

    /**
     * 提取SQL
     */
    private void extractSql(TDsTaskDefinition tDsTaskDefinition){
        // 提取任务参数
        String taskParams = tDsTaskDefinition.getTaskParams();
        JSONObject taskParamJsonObj = JSON.parseObject(taskParams);
        //提取rawScript
        String rawScript = taskParamJsonObj.getString("rawScript");

        //非shell任务节点，没有rawScript
        if(rawScript == null || rawScript.trim().isEmpty()){
            return ;
        }

        //开始位置:
        // 如果有with就找with对应的位置
        // 如果没有with， 就找insert对应的位置
        int startIndex = 0 ;
        int withIndex = rawScript.indexOf("with");  // 如果找不到，返回-1
        if(withIndex  <  0) {
            startIndex = rawScript.indexOf("insert") ;
        }else{
            startIndex = withIndex ;
        }

        if(startIndex == -1 ){
            //表示当前任务中没有SQL
            return ;
        }

        // 结束位置
        //  如果有; ,就照;对应的位置
        //  如果没有; , 就照"对应的位置
        int endIndex = 0 ;
        int fenhaoIndex = rawScript.indexOf(";", startIndex);
        if( fenhaoIndex  < 0 ){
            endIndex = rawScript.indexOf("\"" , startIndex );
        }else{
            endIndex = fenhaoIndex ;
        }

        //取SQL
        String taskSql = rawScript.substring(startIndex, endIndex);

        tDsTaskDefinition.setTaskSql(taskSql) ;
    }
}
