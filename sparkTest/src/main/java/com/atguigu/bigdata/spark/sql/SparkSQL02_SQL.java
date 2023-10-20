package com.atguigu.bigdata.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQL02_SQL {
    public static void main(String[] args) {

        // TODO SparkSQL模块其实就是对Spark Core的一种封装，为了方便操作结构化数据
        //      学习重点：
        //      1. 环境问题（数据源）
        //      2. 模型之间的关系 ： RDD & Dataset
        //      3. 模型的操作 : Dataset方法
        //      4. SQL : no
        //      5. SQL实现不了 : 重点
        final SparkSession session =
            SparkSession.builder().master("local[*]").appName("SparkSQL").getOrCreate();

        final Dataset<Row> jsonRow = session.read().json("data/user.json");

        //jsonRow.show();
        jsonRow.createOrReplaceTempView("user");

        session.sql("select avg(age) from user").show();

        session.stop();

    }
}
