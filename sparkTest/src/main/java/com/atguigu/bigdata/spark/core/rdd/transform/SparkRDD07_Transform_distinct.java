package com.atguigu.bigdata.spark.core.rdd.transform;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SparkRDD07_Transform_distinct {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Transform_groupBy");
        final JavaRDD<String> rdd = sc.parallelize(Arrays.asList(
                "hadoop", "spark", "spark", "hadoop"
        ), 2);

        // TODO distinct 方法用于对数据源的数据进行去重处理
        //      distinct方法底层采用shuffle的方式实现分布式去重
        final JavaRDD<String> distinctRDD = rdd.distinct();

        distinctRDD.collect().forEach(System.out::println);


        sc.stop();
    }
}