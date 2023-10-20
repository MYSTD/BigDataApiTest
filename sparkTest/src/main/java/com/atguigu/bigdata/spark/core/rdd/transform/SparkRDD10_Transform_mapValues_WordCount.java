package com.atguigu.bigdata.spark.core.rdd.transform;

import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class SparkRDD10_Transform_mapValues_WordCount {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Transform_groupBy");

        // TODO 计算 WordCount : 经典教学案例（词频）
        //          商品订单销售总额
        // word1, word2, word1, word2
        // 1. 分组 ：[ word1, word1 ], [word2, word2]
        // 2. 聚合 ：[ word1， 2 ], [ word2， 2 ]
        final JavaRDD<String> rdd = sc.parallelize(
                Arrays.asList(
                        "hello", "hello", "spark", "hadoop", "spark", "hello"
                )
        );

        /*
           groupBy

           ["hadoop", 【hadoop,hadoop,hadoop】]
           ["spark",  【spark,spark,spark】]

           mapValues

           ["hadoop", 【hadoop,hadoop,hadoop】] => (hadoop, 3)
           ["spark",  【spark,spark,spark】]    => (spark,  3)


         */
        /*
          (hello, [hello, hello, hello])
          (spark, [spark, spark])
          (hadoop, [hadoop])
         */
        final JavaPairRDD<String, Iterable<String>> groupRDD = rdd.groupBy(w -> w);

        final JavaPairRDD<String, Long> wordCountRDD = groupRDD.mapValues(
                iter -> iter.spliterator().estimateSize()
        );
        wordCountRDD.collect().forEach(System.out::println);


        sc.stop();
    }
}