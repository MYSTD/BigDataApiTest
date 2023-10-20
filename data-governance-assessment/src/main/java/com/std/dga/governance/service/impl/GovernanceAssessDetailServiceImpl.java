package com.std.dga.governance.service.impl;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.std.dga.assessor.Assessor;
import com.std.dga.dolphinscheduler.bean.TDsTaskDefinition;
import com.std.dga.dolphinscheduler.bean.TDsTaskInstance;
import com.std.dga.dolphinscheduler.service.TDsTaskDefinitionService;
import com.std.dga.dolphinscheduler.service.TDsTaskInstanceService;
import com.std.dga.governance.bean.AssessParam;
import com.std.dga.governance.bean.GovernanceAssessDetail;
import com.std.dga.governance.bean.GovernanceAssessDetailVO;
import com.std.dga.governance.bean.GovernanceMetric;
import com.std.dga.governance.mapper.GovernanceAssessDetailMapper;
import com.std.dga.governance.service.GovernanceAssessDetailService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.std.dga.governance.service.GovernanceMetricService;
import com.std.dga.meta.bean.TableMetaInfo;
import com.std.dga.meta.mapper.TableMetaInfoMapper;
import com.std.dga.util.SpringBeanProvider;
import com.sun.codemodel.internal.JForEach;
import org.apache.tomcat.util.threads.ThreadPoolExecutor;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * <p>
 * 治理考评结果明细 服务实现类
 * </p>
 *
 * @author std
 * @since 2023-10-10
 */
@Service
@DS("dga")
public class GovernanceAssessDetailServiceImpl extends ServiceImpl<GovernanceAssessDetailMapper, GovernanceAssessDetail> implements GovernanceAssessDetailService {

    @Autowired
    TableMetaInfoMapper tableMetaInfoMapper;

    @Autowired
    GovernanceMetricService governanceMetricService;

    @Autowired
    SpringBeanProvider springBeanProvider; // 用于动态获取对象

    @Autowired
    TDsTaskDefinitionService tDsTaskDefinitionService;

    @Autowired
    TDsTaskInstanceService tDsTaskInstanceService;

    /* TODO : JUC 线程池优化：
     *  先按照corePoolSize数量运行，
     *  不够的在BlockingDeque队列等待，队列不够
     *  再开辟新的线程，最多maximumPoolSize个
     */
    ThreadPoolExecutor threadPoolExecutor =
            new ThreadPoolExecutor(20, 30, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<>(2000));


    /**
     *
     * 主考评方法
     *
     * 对每张表， 每个指标， 逐个考评
     *    如何考评？
     *                  将每个指标设计成一个具体的类， 考评器
     *                  例如: 是否有技术OWNER ，  TEC_OWNER    TecOwnerAssessor
     *                  模板设计模式
     *                    整个考评的过程是一致的  ， 通过父类的方法来进行总控。  控制整个考评过程。
     *                    每个指标考评的细节(查找问题)是不同的 ， 通过每个指标对应的考评器(子类) , 实现考评的细节。
     *
     *                  待解决: 如何将指标对应到考评器???（动态获取）
     *                  方案一: 反射的方式:
     *                    约定 : 考评器类的名字与 指标编码 遵循下划线与驼峰的映射规则。
     *                           TEC_OWNER => tec_owner  => TecOwner =>  TecOwnerAssessor
     *                 String code = governanceMetric.getMetricCode().toLowerCase();
     *                 String className = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, code);
     *                 className = className+"Assessor";
     *                 String superPackageName = "com.atguigu.dga.assessor" ;
     *                 String subPackageName = governanceMetric.getGovernanceType().toLowerCase() ;
     *                 String fullClassName = superPackageName +"." +  subPackageName + "." +  className ;
     *                 Assessor assessor = null;
     *                 try {
     *                     assessor = (Assessor)Class.forName(fullClassName).newInstance();
     *                 } catch (Exception e) {
     *                     e.printStackTrace();
     *                 }
     *
     *                 assessor.doAssess();
     *                 方案二: 通过spring容器管理考评器
     *                        每个组件被管理到Spring的容器中后，都有一个默认的名字， 就是类名(首字母小写 )
     *                        也可以显示的指定组件的名字。
     *
     *                        将指标的编码作为考评器的名字来使用。 未来， 获取到一个指标， 就可以通过指标编码从
     *                        容器中获取到对应的考评器对象。
     */
    @Override
    public void mainAssess(String assessDate) {

        // 幂等处理
        remove(
                new QueryWrapper<GovernanceAssessDetail>()
                        .eq("assess_date", assessDate)
        );
//        remove(
//                new QueryWrapper<GovernanceAssessDetail>()
//                        .eq("schema_name" , "gmall")
//        ) ;

        // 1. 读取所有待考评的表
        List<TableMetaInfo> tableMetaInfoList = tableMetaInfoMapper.selectTableMetaInfoList(assessDate);
        System.out.println(tableMetaInfoList);
        HashMap<String, TableMetaInfo> tableMetaInfoMap = new HashMap<>();
        for (TableMetaInfo tableMetaInfo : tableMetaInfoList) {
            tableMetaInfoMap.put(tableMetaInfo.getSchemaName() + "." + tableMetaInfo.getTableName(), tableMetaInfo);
        }

        // 2. 读取所有启用的指标
        List<GovernanceMetric> governanceMetricList = governanceMetricService.list(
                new QueryWrapper<GovernanceMetric>()
                        .eq("is_disabled", "0")
        );

        // 从DS中查询所有的任务定义
        List<TDsTaskDefinition> tDsTaskDefinitions = tDsTaskDefinitionService.selectList();
        // 封装到Map中
        HashMap<String, TDsTaskDefinition> tDsTaskDefinitionHashMap = new HashMap<>();
        for (TDsTaskDefinition tDsTaskDefinition : tDsTaskDefinitions) {
            tDsTaskDefinitionHashMap.put(tDsTaskDefinition.getName(), tDsTaskDefinition);
        }

        // 从DS中查询所有的任务实例
        List<TDsTaskInstance> tDsTaskInstances = tDsTaskInstanceService.selectList(assessDate);
        // 封装到Map中
        HashMap<String, TDsTaskInstance> tDsTaskInstanceHashMap = new HashMap<>();
        for (TDsTaskInstance tDsTaskInstance : tDsTaskInstances) {
            tDsTaskInstanceHashMap.put(tDsTaskInstance.getName(), tDsTaskInstance);
        }


        List<GovernanceAssessDetail> governanceAssessDetailList = new ArrayList<>();
        // 未来要执行的任务集合
        List<CompletableFuture<GovernanceAssessDetail>> futureList = new ArrayList<>();
        for (TableMetaInfo tableMetaInfo : tableMetaInfoList) {
            for (GovernanceMetric governanceMetric : governanceMetricList) {

                // 处理白名单
                String skipAssessTables = governanceMetric.getSkipAssessTables();
                boolean isAssess = true;
                if (skipAssessTables != null && !skipAssessTables.trim().isEmpty()) {
                    String[] skipTables = skipAssessTables.split(",");
                    for (String skipTable : skipTables) {
                        if (skipTable.equals(tableMetaInfo.getTableName())) {
                            isAssess = false;
                            break;
                        }
                    }
                }
                if (!isAssess) continue;

                // 动态获取各考评器组件对象(重点)
                // 也可以通过反射方式获取
                Assessor assessor = springBeanProvider.getBeanByName(governanceMetric.getMetricCode(), Assessor.class);

                // 封装考评参数
                AssessParam assessParam = new AssessParam();
                assessParam.setAssessDate(assessDate);
                assessParam.setTableMetaInfo(tableMetaInfo);
                assessParam.setGovernanceMetric(governanceMetric);
                assessParam.setTableMetaInfoMap(tableMetaInfoMap);

                // 封装DS中的task定义与实例
                assessParam.setTDsTaskDefinition(tDsTaskDefinitionHashMap.get(tableMetaInfo.getSchemaName() + "." + tableMetaInfo.getTableName()));
                assessParam.setTDsTaskInstance(tDsTaskInstanceHashMap.get(tableMetaInfo.getSchemaName() + "." + tableMetaInfo.getTableName()));

                // 开始考评
//                GovernanceAssessDetail governanceAssessDetail = assessor.doAssessor(assessParam);
//
//                governanceAssessDetailList.add(governanceAssessDetail);

                // JUC 异步改造优化，做任务编排
                CompletableFuture<GovernanceAssessDetail> future = CompletableFuture.supplyAsync(
                        () -> {
                            // 考评
                            return assessor.doAssessor(assessParam);
                        },
                        threadPoolExecutor
                );

                futureList.add(future);
            }
        }
        // 异步计算， 最后集结， 统一计算
        governanceAssessDetailList = futureList
                .stream().map(CompletableFuture::join).collect(Collectors.toList());

        // 批写
        saveBatch(governanceAssessDetailList);

    }


    /**
     * 获取问题列表
     * <p>
     * 页面的请求： http://dga.gmall.com/governance/problemList/SPEC/1/5
     * <p>
     * 返回的结果:
     * [
     * {"assessComment":"","assessDate":"2023-05-01","assessProblem":"缺少技术OWNER","assessScore":0.00,"commentLog":"",
     * "createTime":1682954933000,"governanceType":"SPEC",
     * "governanceUrl":"/table_meta/table_meta/detail?tableId=1803","id":21947,
     * "isAssessException":"0","metricId":1,"metricName":"是否有技术Owner",
     * "schemaName":"gmall","tableName":"ads_page_path"}
     * ,
     * {...},....
     * ]
     */
    @Override
    public List<GovernanceAssessDetailVO> getProblemList(String governanceType, Integer pageNo, Integer pageSize) {
        int start = (pageNo - 1) * pageSize;
        List<GovernanceAssessDetail> governanceAssessDetailList = list(
                new QueryWrapper<GovernanceAssessDetail>()
                        .eq("governance_type", governanceType)
                        .lt("assess_score", 10)
                        .inSql("assess_date", "select max(assess_date) from governance_assess_detail")
                        .last("limit " + start + "," + pageSize));

        // 封装到VO中
        List<GovernanceAssessDetailVO> governanceAssessDetailVOList = new ArrayList<>();
        for (GovernanceAssessDetail governanceAssessDetail : governanceAssessDetailList) {
            GovernanceAssessDetailVO governanceAssessDetailVO = new GovernanceAssessDetailVO();
            // 按属性名称copy值
            BeanUtils.copyProperties(governanceAssessDetail, governanceAssessDetailVO);
            governanceAssessDetailVOList.add(governanceAssessDetailVO);
        }
        return governanceAssessDetailVOList;
    }

    /**
     * 获取问题个数
     *
     * 返回结果: {"SPEC":1, "STORAGE":4,"CALC":12,"QUALITY":34,"SECURITY":12}
     *
     * listMaps() 返回的是一个List<Map<String, Object>>集合，
     * list集合中的每个map集合代表一条数据，k为字段名，v为字段值
     */
    @Override
    public Map<String, Long> getProblemNum() {
        List<Map<String, Object>> problemNumMaps = listMaps(new QueryWrapper<GovernanceAssessDetail>()
                .select("governance_type", "count(*) cnt")
                .lt("assess_score", 10)
                .inSql("assess_date", "select max(assess_date) from governance_assess_detail")
                .groupBy("governance_type"));
        HashMap<String, Long> resultMap = new HashMap<>();

        for (Map<String, Object> problemNumMap : problemNumMaps) {
            resultMap.put(problemNumMap.get("governance_type").toString(),(Long) problemNumMap.get("cnt"));
        }
        return resultMap;
    }
}
