package com.atguigu.bigdata.spark.core.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class SparkCore01_Env {
    public static void main(String[] args) {

        // TODO 构建Spark框架的运行环境
        //   1. 获取环境的对象
        //final JavaSparkContext sc = new JavaSparkContext("local[*]", "SparkCoreEnv");
        // 构建环境对象
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("SparkCoreEnv");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //   2. 通过环境对象获取RDD对象
        final JavaRDD<String> rdd = sc.textFile("data/word.txt");

        //   3. 通过RDD对象进行数据操作
        final List<String> collect = rdd.collect();

        //   4. 获取结果
        collect.forEach(System.out::println);

        //   5. 释放资源
        sc.stop();

    }
}
