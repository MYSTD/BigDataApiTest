package com.atguigu.bigdata.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQL05_Dataset_SQL {
    public static void main(String[] args) {

        // TODO Dataset : SparkSQL的数据出来模型，用于封装RDD的
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("SPARK_SQL");

        // TODO SparkSQL的环境对象，称之为SparkSession
        //      session : 会话，类似于JDBC当中Connection
        final SparkSession session =
            SparkSession.builder()
                .config(conf)
                .getOrCreate();

        // TODO 对接数据源，可以构建数据模型
        final Dataset<Row> jsonDS = session.read().json("data/user.json");

        // TODO 使用SQL的方式访问Dataset
        //      需要将Dataset转换成表(视图)
        //      Table : 表，可以操作数据
        //      View  : 视图，查询结果集，无法操作的
        //      TMEMP表示临时操作，这个临时操作只对当前Session有效
        jsonDS.createOrReplaceTempView("user");
        //session.newSession().sql("select * from user").show();


        session.stop();

    }
}