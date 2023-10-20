package com.std.dga.assessor.calc;

import avro.shaded.com.google.common.collect.Sets;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.std.dga.assessor.Assessor;
import com.std.dga.governance.bean.AssessParam;
import com.std.dga.governance.bean.GovernanceAssessDetail;
import com.std.dga.meta.bean.TableMetaInfo;
import com.std.dga.util.SqlUtil;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

@Component("SIMPLE_PROCESS")
public class SimpleProcessAssessor extends Assessor {

    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws Exception {
        // 排除ODS层的表
        if (assessParam.getTableMetaInfo().getTableMetaInfoExtra().getDwLevel().equals("ODS")) {
            return;
        }
        // 排除没有SQL的表
        if (assessParam.getTDsTaskDefinition() == null || assessParam.getTDsTaskDefinition().getTaskSql() == null) {
            return;
        }

        // 提取sql
        String taskSql = assessParam.getTDsTaskDefinition().getTaskSql();
        // 解析sql
        SimpleProcessDispatcher simpleProcessDispatcher = new SimpleProcessDispatcher();
        simpleProcessDispatcher.setDefaultSchemaName(assessParam.getTableMetaInfo().getSchemaName());
        SqlUtil.parseSql(taskSql,simpleProcessDispatcher);

        //提取 复杂计算集合  被查询的表  过滤的字段
        Set<String> sqlComplicateSet = simpleProcessDispatcher.getSqlComplicateSet();
        Set<String> sqlTableRef = simpleProcessDispatcher.getSqlTableRef();
        Set<String> sqlFilterCol = simpleProcessDispatcher.getSqlFilterCol();

        //判断是否有复杂计算
        if(sqlComplicateSet.size() > 0 ){
            governanceAssessDetail.setAssessComment("sql中涉及的复杂计算: " + sqlComplicateSet);
            return ;
        }

        // 提取所有表的分区字段名
        HashSet<String> allTablePartitionColSet = new HashSet<>();
        for (String tableName : sqlTableRef) {
            TableMetaInfo tableMetaInfo = assessParam.getTableMetaInfoMap().get(tableName);
            // 获取分区字段JSON
            String partitionColNameJson = tableMetaInfo.getPartitionColNameJson();
            List<JSONObject> jsonObjectList = JSON.parseArray(partitionColNameJson, JSONObject.class);
            // 获取分区字段名
            Set<String> partitionColSet = jsonObjectList.stream()
                    .map(jsonObject -> jsonObject.getString("name"))
                    .collect(Collectors.toSet());
            allTablePartitionColSet.addAll(partitionColSet);
        }

        Boolean isPartitionCol = true ;
        //判断sql中的过滤字段是否有非分区字段
        for (String filterCol : sqlFilterCol) {
            if(!allTablePartitionColSet.contains(filterCol)){
                isPartitionCol = false ;
                break ;
            }
        }

        if(isPartitionCol){
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("所有的过滤字段都是分区字段");
        }

        governanceAssessDetail.setAssessComment("过滤字段为: " + sqlFilterCol + " , 分区字段为: " + allTablePartitionColSet );



    }

    public static class SimpleProcessDispatcher implements Dispatcher {

        @Setter
        //默认的库名
        private String defaultSchemaName;

        //复杂计算涉及的操作
        Set<Integer> complicateTokSet = Sets.newHashSet(
                HiveParser.TOK_JOIN,   // join ,包含通过where连接的情况
                HiveParser.TOK_GROUPBY, // group by
                HiveParser.TOK_LEFTOUTERJOIN, // left join
                HiveParser.TOK_RIGHTOUTERJOIN, //right join
                HiveParser.TOK_FULLOUTERJOIN, // full join
                HiveParser.TOK_FUNCTION, // count(1)
                HiveParser.TOK_FUNCTIONDI, // count(distinct xx)
                HiveParser.TOK_FUNCTIONSTAR, // count(*)
                HiveParser.TOK_SELECTDI, // distinct
                HiveParser.TOK_UNIONALL // union
        );

        // 记录sql中实际存在的复杂计算
        @Getter
        Set<String> sqlComplicateSet = new HashSet<>();

        //记录sql中所有被查询的表
        @Getter
        Set<String> sqlTableRef = new HashSet<>();

        //记录所有的过滤字段
        @Getter
        Set<String> sqlFilterCol = new HashSet<>();

        // where后面的操作符号
        Set<Integer> operatorSet = Sets.newHashSet(
                HiveParser.EQUAL,
                HiveParser.GREATERTHAN,
                HiveParser.LESSTHAN,
                HiveParser.GREATERTHANOREQUALTO,
                HiveParser.LESSTHANOREQUALTO,
                HiveParser.NOTEQUAL,
                HiveParser.KW_LIKE
        );

        @Override
        public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs) throws SemanticException {
            // 转换为子类型
            ASTNode astNode = (ASTNode) nd;

            // 判断是否存在复杂计算
            if (complicateTokSet.contains(astNode.getType()))
                sqlComplicateSet.add(astNode.getToken().getText());

            // 提取所有被查询的表
            if( astNode.getType() == HiveParser.TOK_TABNAME){
                //判断当前节点有几个子节点
                if(astNode.getChildren().size() == 1){
                    // 1个孩子， 就是表名
                    sqlTableRef.add( defaultSchemaName + "." +  astNode.getChild(0).getText()) ;
                }else{
                    // 2个孩子， 第一个是库名  第二个是表名
                    sqlTableRef.add( astNode.getChild(0).getText() + "." + astNode.getChild(1).getText()) ;
                }
            }

            //提取所有被过滤的字段
            //找到Sql中的操作符号， 并判断是否属于where的孩子， 如果是，就定性为过滤条件
            if(operatorSet.contains(astNode.getType()) && astNode.getAncestor(HiveParser.TOK_WHERE) != null ){

                ASTNode childAstNode = (ASTNode)astNode.getChild(0);
                //判断当前节点下是 Tok_table_or_col 还是 "."
                if(childAstNode.getType() == HiveParser.TOK_TABLE_OR_COL){
                    sqlFilterCol.add( childAstNode.getChild(0).getText()) ;
                }else if( childAstNode.getType() == HiveParser.DOT){
                    sqlFilterCol.add( childAstNode.getChild(1).getText()) ;
                }
            }

            return null;
        }
    }
}
