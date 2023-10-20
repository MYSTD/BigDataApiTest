package com.atguigu.bigdata.spark.core.rdd.part;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkRDD02_Part_DIY {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Transform_groupBy");
        final JavaPairRDD<String, String> rdd = sc.parallelizePairs(
                Arrays.asList(
                        new Tuple2<>("nba", "xxxxx"),
                        new Tuple2<>("cba", "xxxxx"),
                        new Tuple2<>("nba", "xxxxx"),
                        new Tuple2<>("wnba", "xxxxx")
                ),2
        );

        final JavaPairRDD<String, Iterable<String>> groupRDD = rdd.groupByKey(
                new MyPartitioner()
        );

        groupRDD.saveAsTextFile("output");

        sc.stop();
    }
}