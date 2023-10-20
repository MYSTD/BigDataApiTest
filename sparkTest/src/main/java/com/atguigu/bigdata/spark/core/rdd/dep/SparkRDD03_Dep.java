package com.atguigu.bigdata.spark.core.rdd.dep;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkRDD03_Dep {
    public static void main(String[] args) throws Exception {

        // TODO RDD对象之间会形成依赖关系，连续的依赖关系称之为血缘关系
        //      每个RDD都包含血缘关系
        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Action");

        // Stage = 1(2) + 1(3) = 2(5)
        final JavaRDD<Integer> rdd = sc.parallelize(
                Arrays.asList(
                        1, 2, 3,4
                ), 4
        );

        final JavaPairRDD<Integer, Integer> mapRDD = rdd.mapToPair(
                num -> new Tuple2<>(num, num)
        );

        // coalesce方法默认可以在不shuffle的情况下，改变分区数量
        final JavaPairRDD<Integer, Integer> coalesceRDD = mapRDD.coalesce(2);


        final JavaPairRDD<Integer, Integer> reduceRDD = coalesceRDD.reduceByKey(Integer::sum, 3);
        reduceRDD.collect().forEach(System.out::println);

        Thread.sleep(999999999);

        sc.stop();
    }
}
