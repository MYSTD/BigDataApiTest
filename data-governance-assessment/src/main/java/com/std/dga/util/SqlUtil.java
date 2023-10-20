package com.std.dga.util;

import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

import java.util.Collections;

/**
 * ClassName:SqlUtil
 * Description:
 *
 * @date:2023/10/8 11:09
 * @author:STD
 */
public class SqlUtil {

    public static String filterUnsafeSql(String input) {
        if (input == null) {
            return null;
        }

        // 替换 MySQL 中可能导致 SQL 注入的特殊字符
        return input.replace("\\", "\\\\")
                .replace("'", "\\'")
                .replace("\"", "\\\"")
                .replace("\b", "\\b")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t")
                .replace("\u001A", "\\Z")
                .replace("%", "\\%")
                .replace("_", "\\_");
    }

    /**
     * 将SQL解析成语法树，并进行处理
     *
     * SQL转成AST的过程，不需要我们管， 有现成的实现，我们使用hive的实现
     *
     * SQL转成AST后，需要对整个树进行迭代处理， 迭代的过程也不需要我们管，但是我们需要自定义每个节点的处理过程。
     *
     * 可以通过Dispatcher来实现自定义的处理过程
     *
     * @param  sql   被解析的SQL
     * @param  dispatcher  自定义的节点处理器
     */
    public static void parseSql(String sql , Dispatcher dispatcher) throws Exception {
        //将SQL解析成AST
        ParseDriver parseDriver = new ParseDriver();
        ASTNode astNode = parseDriver.parse(sql);

        //将nil节点去掉， 将TOK_QUERY作为根节点
        while(astNode.getType() != HiveParser.TOK_QUERY){
            // 将第一个孩子节点作为根节点
            astNode = (ASTNode) astNode.getChild(0);
        }

        //把处理器放到遍历器中
        DefaultGraphWalker graphWalker = new DefaultGraphWalker(dispatcher);
        //Collections.singleton()只有一个值的不可变集合
        //对树进行遍历处理
        graphWalker.startWalking(Collections.singleton(astNode), null);

    }

    public static void main(String[] args) throws Exception {
        String sql = " select  count(*)  from  test.user_info  ui  join order_info oi on ui.id = oi.id where ui.uname = 'abc' group by oi.name " ;

        System.out.println("------------");
    }
}

