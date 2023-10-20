package com.std.dga.dolphinscheduler.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.std.dga.dolphinscheduler.bean.TDsTaskInstance;
import com.std.dga.dolphinscheduler.mapper.TDsTaskInstanceMapper;
import com.std.dga.dolphinscheduler.service.TDsTaskInstanceService;
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
public class TDsTaskInstanceServiceImpl extends ServiceImpl<TDsTaskInstanceMapper, TDsTaskInstance> implements TDsTaskInstanceService {

    @Override
    public List<TDsTaskInstance> selectList(String assessDate) {
        List<TDsTaskInstance> tDsTaskInstanceList = list(
                new QueryWrapper<TDsTaskInstance>().exists(
                        "SELECT 1 FROM ( " +
                                "     SELECT MAX(id) max_id FROM t_ds_task_instance " +
                                "     WHERE state=7 AND DATE_FORMAT(start_time , '%Y-%m-%d') = '" + assessDate + "' " +
                                "     GROUP BY NAME  " +
                                "  )t2  " +
                                "  WHERE t_ds_task_instance.id =  t2.max_id"
                )
        );
        return tDsTaskInstanceList;
    }

    @Override
    public List<TDsTaskInstance> selectFailedTask(String taskName, String assessDate) {
        List<TDsTaskInstance> list = list(new QueryWrapper<TDsTaskInstance>()
                .eq("name", taskName)
                .eq("date_format(start_time , '%Y-%m-%d')", assessDate)
                .eq("state", 6));
        return list;
    }

    @Override
    public List<TDsTaskInstance> selectBeforeNDaysInstance(String taskName, String startDate, String assessDate) {
        // 以同一任务当天执行成功的最大id作为这一任务的执行情况
        List<TDsTaskInstance> beforeList = list(
                new QueryWrapper<TDsTaskInstance>()
                        .exists(
                                "SELECT 1 FROM (\n" +
                                        "SELECT MAX(id) max_id FROM t_ds_task_instance\n" +
                                        "WHERE state=7 \n" +
                                        "AND \n" +
                                        "  DATE_FORMAT(start_time , '%Y-%m-%d') >= '" + startDate + "'\n" +
                                        "AND\n" +
                                        "  DATE_FORMAT(start_time , '%Y-%m-%d') < '" + assessDate + "'  \n" +
                                        "AND   \n" +
                                        "  NAME = '" + taskName + "'\n" +
                                        "  \n" +
                                        "GROUP BY NAME , DATE_FORMAT(start_time , '%Y-%m-%d')\n" +
                                        ") t1 \n" +
                                        "WHERE t_ds_task_instance.id = t1.max_id "
                        )
        );
        return beforeList ;
    }
}
