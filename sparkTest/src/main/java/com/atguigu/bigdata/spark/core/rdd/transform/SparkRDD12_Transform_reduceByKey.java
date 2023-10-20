package com.atguigu.bigdata.spark.core.rdd.transform;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class SparkRDD12_Transform_reduceByKey {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Transform_groupByKey");

        final JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(
                Arrays.asList(
                        new Tuple2<>("hello", 1),
                        new Tuple2<>("hello", 2),
                        new Tuple2<>("spark", 3),
                        new Tuple2<>("spark", 4),
                        new Tuple2<>("hello", 5)
                )
        );

        // TODO reduceByKey方法：将相同key的数据中的v进行两两聚合
        //      reduceByKey方法等同于对groupByKey和聚合功能进行了优化和简化，推荐使用。
        //     (hello, 1), (hello, 2), (hello, 5)
        //     (hello,[1,2,5])
        //     (spark,3), (spark, 4)
        //     (spark, [3,4])
        rdd.reduceByKey(Integer::sum)
                .collect().forEach(System.out::println);



        sc.stop();
    }
}