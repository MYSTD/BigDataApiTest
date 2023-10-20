package com.std.dga.governance.controller;

import com.std.dga.governance.bean.GovernanceAssessDetailVO;
import com.std.dga.governance.service.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

/**
 * ClassName:MainGovernanceServiceController
 * Description:
 *
 * @date:2023/10/16 14:30
 * @author:STD
 */
@RestController
@RequestMapping("/governance")
public class MainGovernanceServiceController {

    @Autowired
    GovernanceAssessTableService governanceAssessTableService;
    @Autowired
    GovernanceAssessTecOwnerService governanceAssessTecOwnerService;
    @Autowired
    GovernanceAssessGlobalService governanceAssessGlobalService;
    @Autowired
    GovernanceAssessDetailService governanceAssessDetailService;
    @Autowired
    MainGovernanceService mainGovernanceService;

    /**
     * 获取全局分数
     *
     * 返回结果：{"assessDate":"2023-04-01" ,"sumScore":90, "scoreList":[20,40,34,55,66]}
     */
    @GetMapping("/globalScore")
    public Map<String,Object> globalScore(){
        return governanceAssessGlobalService.getLastGlobalScore();
    }

    /**
     * 获取人员排名列表
     *
     * 返回结果：
     * [{"tecOwner":"zhang3" ,"score":99},
     * {"tecOwner":"li4" ,"score":98},
     * {"tecOwner": "wang5","score":97}]
     */
    @GetMapping("/rankList")
    public List<Map<String,Object>> rankList(){
        return governanceAssessTecOwnerService.getRankList();
    }

    /**
     * 获取问题列表
     *
     * 页面的请求： http://dga.gmall.com/governance/problemList/SPEC/1/5
     */
    @GetMapping("/problemList/{governanceType}/{pageNo}/{pageSize}")
    public List<GovernanceAssessDetailVO> problemList (@PathVariable("governanceType") String governanceType ,
                                                       @PathVariable("pageNo") Integer pageNo,
                                                       @PathVariable("pageSize") Integer pageSize ) throws InvocationTargetException, IllegalAccessException {
        return  governanceAssessDetailService.getProblemList(governanceType , pageNo , pageSize);
    }

    /**
     * 获取各类型问题数量
     */
    @GetMapping("/problemNum")
    public Map<String,Long> problemNum(){
        return governanceAssessDetailService.getProblemNum();
    }

    /**
     * 手动执行评估
     */
    @PostMapping("/assess/{date}")
    public String assess(@PathVariable("date") String assessDate) throws Exception {
        mainGovernanceService.restartGovernance(assessDate);
        return "success";
    }



}
