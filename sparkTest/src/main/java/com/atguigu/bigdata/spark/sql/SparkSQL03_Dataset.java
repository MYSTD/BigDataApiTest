package com.atguigu.bigdata.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

public class SparkSQL03_Dataset {
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

        // TODO as方法可以用于将数据进行处理的数据类型进行转换
        //      SparkSQL从文件中获取的数值类型会全部当成Long（BigInt）类型处理
        final Dataset<User> userDS = jsonDS.as(Encoders.bean(User.class));

        userDS.show();

        // TODO Dataset数据模型本质其实就是封装了RDD
        // JDBC : ResultSet.getXXX
        //final JavaRDD<Row> rowJavaRDD = jsonDS.javaRDD();
        //final JavaRDD<User> userJavaRDD = userDS.javaRDD();

        session.stop();

    }
}
class User implements Serializable {
    private String name;
    private long age;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getAge() {
        return age;
    }

    public void setAge(long age) {
        this.age = age;
    }
}
