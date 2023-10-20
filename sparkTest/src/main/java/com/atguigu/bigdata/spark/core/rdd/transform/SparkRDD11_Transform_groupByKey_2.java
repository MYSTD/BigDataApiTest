package com.atguigu.bigdata.spark.core.rdd.transform;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class SparkRDD11_Transform_groupByKey_2 {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Transform_groupByKey");

        final JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(
                Arrays.asList(
                        new Tuple2<>("hello", 1),
                        new Tuple2<>("hello", 1),
                        new Tuple2<>("spark", 1),
                        new Tuple2<>("spark", 1),
                        new Tuple2<>("hello", 1)
                )
        );
        // (hello, 8)
        // (spark, 7)
        // 分组
        // ( hello,[1, 2, 5] )
        // ( spark,[3, 4] )
        rdd.groupByKey()
            //.collect().forEach(System.out::println);
        // 聚合
        .mapValues(
            iter -> {
                int sum = 0;
                final Iterator<Integer> iterator = iter.iterator();
                while ( iterator.hasNext() ) {
                    final Integer num = iterator.next();
                    sum += num;
                }
                return sum;
            }
        )
        .collect().forEach(System.out::println);



        sc.stop();
    }
}