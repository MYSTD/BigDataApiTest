package com.atguigu.bigdata.spark.core.rdd.transform;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkRDD09_Transform_KV {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Transform_groupBy");
        final JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(
                1,2,3,4,5,6
        ), 3);

        // TODO num -> (num, num)
        //    单值类型的数据：把数据当成一个单一整体数据
        //    键值类型的数据：把数据分成k,v两个独立的概念使用
        final JavaRDD<Tuple2<Integer, Integer>> mapRDD = rdd.map(
                num -> new Tuple2<Integer, Integer>(num, num)
        );

        final JavaPairRDD<Integer, Integer> pairRDD = rdd.mapToPair(num -> new Tuple2<Integer, Integer>(num, num));

        mapRDD.collect().forEach(System.out::println);
        System.out.println("***************************");
        pairRDD.collect().forEach(System.out::println);


        sc.stop();
    }
}