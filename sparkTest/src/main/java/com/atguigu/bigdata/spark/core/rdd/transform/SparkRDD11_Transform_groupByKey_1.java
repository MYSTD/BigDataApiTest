package com.atguigu.bigdata.spark.core.rdd.transform;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkRDD11_Transform_groupByKey_1 {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Transform_groupByKey");


        final JavaRDD<String> rdd = sc.parallelize(
                Arrays.asList(
                        "a", "b", "a", "b"
                )
        );

        final JavaPairRDD<String, Iterable<String>> groupRDD = rdd.groupBy(s -> s);
        // TODO groupBy方法的底层使用的就是groupByKey，groupByKey方法底层就含有shuffle的操作
        //groupRDD.groupByKey()

        // ( a, [a, a] )
        // ( b, [b, b] )
        // this.map(t => (cleanF(t), t)).groupByKey(p)
        // a => (a, a)
        // b => (b, b)
        // a => (a, a)
        // b => (b, b)
        // [ (a, a), (b, b), (a, a), (b, b) ]
        // ( a, [a, a] )
        // ( b, [b, b] )
        groupRDD.collect().forEach(System.out::println);


        sc.stop();
    }
}