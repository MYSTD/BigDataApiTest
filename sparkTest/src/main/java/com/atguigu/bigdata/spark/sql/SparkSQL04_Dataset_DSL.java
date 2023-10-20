package com.atguigu.bigdata.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

public class SparkSQL04_Dataset_DSL {
    public static void main(String[] args) {

        // TODO Dataset : SparkSQL的数据出来模型，用于封装RDD的
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("SPARK_SQL");

        final SparkSession session =
            SparkSession.builder()
                .config(conf)
                .getOrCreate();

        // TODO 对接数据源，可以构建数据模型
        final Dataset<Row> jsonDS = session.read().json("data/user.json");

        // SQL => Hibernate => save(new User()) => SSM

        // TODO Dataset模型除了和RDD的使用方式相同以外，还提供其他的访问方式
        //   1. DSL : 类似于SQL的方法
        //   2. SQL : select name from user where id = 1
        jsonDS.select("name", "age").show();


        session.stop();

    }
}