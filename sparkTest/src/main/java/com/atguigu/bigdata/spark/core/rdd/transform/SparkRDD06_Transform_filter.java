package com.atguigu.bigdata.spark.core.rdd.transform;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SparkRDD06_Transform_filter {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Transform_groupBy");
        final JavaRDD<String> rdd = sc.parallelize(Arrays.asList(
                "hive", "spark", "Sello", "hadoop"
        ), 2);

        // TODO filter方法用于对数据源中的数据根据条件进行筛选过滤
        //      满足条件（true）的数据保留，不满足条件(false)数据丢弃

        // filter方法有可能会出现数据倾斜
        //   1. 扩容
        //   2. shuffle
        final JavaRDD<String> filterRDD = rdd.filter(
                s -> {
                    String headString = s.substring(0,1);
                    return headString.equalsIgnoreCase("s");
                }
        );

        filterRDD.collect().forEach(System.out::println);


        sc.stop();
    }
}