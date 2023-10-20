package com.atguigu.bigdata.spark.core.rdd.transform;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SparkRDD05_Transform_groupBy {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Transform_groupBy");

        // TODO 对接内存数据源
        final JavaRDD<String> rdd = sc.parallelize(Arrays.asList(
                "hello", "hello", "hello", "hello"
        ), 2);

        // TODO map方法作用：1个输入 -> 1个输出
        // TODO flatMap方法作用：1个输入 -> N个输出
        //     Spark中的分组操作，必须保证一个组的数据放置在一个分区中
        // groupBy方法会将数据按照指定的规则进行分组
        //   分组后的结果格式为：（组，【数据】）
        final JavaPairRDD<String, Iterable<String>> groupByRDD = rdd.groupBy(
                s -> s.substring(0, 1)
        );

        //groupByRDD.collect().forEach(System.out::println);
        groupByRDD.saveAsTextFile("output");


        sc.stop();
    }
}