package com.atguigu.bigdata.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQL06_Dataset_Execute {
    public static void main(String[] args) throws Exception {

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

        jsonDS.show();

        Thread.sleep(999999999);



        session.stop();

    }
}