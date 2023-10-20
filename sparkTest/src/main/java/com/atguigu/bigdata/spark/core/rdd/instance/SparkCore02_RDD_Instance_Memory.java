package com.atguigu.bigdata.spark.core.rdd.instance;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkCore02_RDD_Instance_Memory {
    public static void main(String[] args) {

        // TODO 构建环境对象
        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDDInstance_Memory");

        // 1. 对接内存数据源（集合），获取RDD对象
        List<String> dataList = Arrays.asList("zhangsan", "lisi", "wangwu");
        // parallelize : 并行
        final JavaRDD<String> rdd = sc.parallelize(dataList);

        rdd.collect().forEach(System.out::println);

        sc.stop();
    }
}
