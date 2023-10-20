package com.atguigu.bigdata.spark.core.rdd.persist;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SparkRDD01_Persist {
    public static void main(String[] args) throws Exception {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Persist");

        // TODO 先完成，再完美，最后完美的完成！
        final JavaRDD<String> rdd1 = sc.textFile("data/word.txt");
        final JavaPairRDD<String, Integer> mapPairRDD1 = rdd1.mapToPair(line -> {
            System.out.println("##########");
            return new Tuple2<>(line, 1);
        });
        final JavaPairRDD<String, Integer> reduceRDD = mapPairRDD1.reduceByKey(Integer::sum);
        reduceRDD.collect();//.forEach(System.out::println);
        System.out.println("*******************************************");
        final JavaRDD<String> rdd2 = sc.textFile("data/word.txt");
        final JavaPairRDD<String, Integer> mapPairRDD2 = rdd2.mapToPair(line -> {
            System.out.println("%%%%%%%%");
            return new Tuple2<>(line, 1);
        });
        final JavaPairRDD<String, Iterable<Integer>> groupByRDD2 = mapPairRDD2.groupByKey();
        groupByRDD2.collect();//.forEach(System.out::println);

        sc.stop();
    }
}
