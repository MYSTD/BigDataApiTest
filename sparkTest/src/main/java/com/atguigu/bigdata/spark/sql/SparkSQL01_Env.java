package com.atguigu.bigdata.spark.sql;

import org.apache.spark.sql.SparkSession;

public class SparkSQL01_Env {
    public static void main(String[] args) {

        // TODO SparkSQL是Spark框架中的一个功能模块
        //      SparkSQL的核心目的是简化Spark Core的代码开发以及简化特殊格式数据的解析
        //      1. 使用SQL的方式来代替之前的RDD开发
        //      2. 将特殊格式的数据转换为表，使用SQL访问
        //      SparkSQL功能模块其实就是对Spark Core的一种封装
        //      所以很多对象都进行了封装
        //      1. 环境对象
        //          Spark Core : JavaSparkContext
        //          Spark SQL :  JavaSparkSession
        //      2. 数据模型
        //          Spark Core : RDD
        //          Spark SQL  : Dataset

        // TODO SparkSQL中的环境对象不是直接new出来的。采用特殊的设计模式（构建器）构造出来的。
        //SparkSession session = new SparkSession();
        final SparkSession session =
            SparkSession.builder().master("local[*]").appName("SparkSQL").getOrCreate();

        session.stop();

    }
}
