package com.atguigu.bigdata.spark.core.rdd.part;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SparkRDD01_Part_sortBy {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Transform_groupBy");
        final JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(
                1,2,3,4,99,100
        ), 3);

        final JavaRDD<Integer> sortByRDD = rdd.sortBy(num -> num, true, 2);

        //sortByRDD.collect().forEach(System.out::println);
        sortByRDD.saveAsTextFile("output");



        sc.stop();
    }
}