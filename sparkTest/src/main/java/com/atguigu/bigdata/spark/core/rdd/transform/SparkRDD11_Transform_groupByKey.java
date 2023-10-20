package com.atguigu.bigdata.spark.core.rdd.transform;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkRDD11_Transform_groupByKey {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Transform_groupByKey");

        final JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(
                Arrays.asList(
                        new Tuple2<>("a", 1),
                        new Tuple2<>("a", 2),
                        new Tuple2<>("b", 3),
                        new Tuple2<>("b", 4)
                )
        );

        // TODO groupBy : 给数据增加分组标记的时候，标记可以为任意值
        //                分组后的数据集合:将数据本身
        //      groupByKey : 给数据增加分组标记的时候,标记只能是key
        //                分组后的数据集合:将Value进行了分组

        //final JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupRDD1 = rdd.groupBy(t -> t._1);
        final JavaPairRDD<String, Iterable<Integer>> groupRDD2 = rdd.groupByKey();
        groupRDD2.collect().forEach(System.out::println);


        sc.stop();
    }
}