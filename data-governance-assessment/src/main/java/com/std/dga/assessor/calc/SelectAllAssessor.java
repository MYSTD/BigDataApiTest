package com.std.dga.assessor.calc;

import com.std.dga.assessor.Assessor;
import com.std.dga.governance.bean.AssessParam;
import com.std.dga.governance.bean.GovernanceAssessDetail;
import com.std.dga.util.SqlUtil;
import lombok.Getter;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Stack;

@Component("SELECT_ALL")
public class SelectAllAssessor extends Assessor {

    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws Exception {

        // 排除ODS层的表
        if(assessParam.getTableMetaInfo().getTableMetaInfoExtra().getDwLevel().equals("ODS")){
            return ;
        }
        // 排除没有SQL的表
        if(assessParam.getTDsTaskDefinition() == null || assessParam.getTDsTaskDefinition().getTaskSql() == null ){
            return ;
        }

        // 提取sql
        String taskSql = assessParam.getTDsTaskDefinition().getTaskSql();

        // 解析
        SelectAllDispatcher selectAllDispatcher = new SelectAllDispatcher();
        SqlUtil.parseSql(taskSql,selectAllDispatcher);

        if(selectAllDispatcher.getIsContainsSelectAll()){
            // 包含select *
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("Sql中包含select *");
        }

    }

    public static class SelectAllDispatcher implements Dispatcher {

        @Getter
        private Boolean  isContainsSelectAll = false;
        @Override
        public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs) throws SemanticException {
            // 转换为子类型
            ASTNode astNode = (ASTNode) nd;

            //判断当前节点是否为 select *
            if(astNode.getType() == HiveParser.TOK_ALLCOLREF){
                isContainsSelectAll = true ;
            }
            return null;
        }
    }
}


