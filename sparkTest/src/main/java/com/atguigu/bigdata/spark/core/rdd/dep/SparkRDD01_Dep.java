package com.atguigu.bigdata.spark.core.rdd.dep;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SparkRDD01_Dep {
    public static void main(String[] args) throws Exception {

        // TODO RDD对象之间会形成依赖关系，连续的依赖关系称之为血缘关系
        //      每个RDD都包含血缘关系
        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Action");

        final JavaRDD<Integer> rdd = sc.parallelize(
                Arrays.asList(
                        1, 2, 3, 4, 5, 6
                ), 2
        );
        System.out.println(rdd.toDebugString());
        System.out.println("********************************");
        final JavaPairRDD<Integer, Integer> mapRDD = rdd.mapToPair(
                num -> new Tuple2<>(num, num)
        );
        System.out.println(mapRDD.toDebugString());
        System.out.println("********************************");
        final JavaPairRDD<Integer, Integer> reduceRDD = mapRDD.reduceByKey(Integer::sum);
        System.out.println(reduceRDD.toDebugString());
        System.out.println("********************************");
        reduceRDD.collect();//.forEach(System.out::println);


        sc.stop();
    }
}
