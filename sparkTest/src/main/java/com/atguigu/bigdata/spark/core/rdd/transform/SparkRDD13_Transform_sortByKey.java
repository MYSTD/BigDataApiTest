package com.atguigu.bigdata.spark.core.rdd.transform;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkRDD13_Transform_sortByKey {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Transform_groupByKey");

        final JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(
                Arrays.asList(
                        new Tuple2<>("a", 1),
                        new Tuple2<>("b", 4),
                        new Tuple2<>("a", 5),
                        new Tuple2<>("b", 6),
                        new Tuple2<>("a", 3),
                        new Tuple2<>("b", 2)
                )
        );

        // TODO sortBy方法：按照指定的规则对数据排序
       // rdd.sortBy
        // TODO sortByKey方法：按照数据中的k对数据排序
        // RDD中可以流转不同的数据，数据类型没有特殊的要求
        //  sortByKey方法需要流转的数据可以进行比较（实现可比较接口）
        //  sortByKey方法默认可以不传递任何参数，那么默认的排序规则就是升序
        //  sortByKey方法底层也存在shuffle操作
        final JavaPairRDD<String, Integer> sortRDD = rdd.sortByKey();
        // 1, 5, 3, ,4, 6, 2
        sortRDD.collect().forEach(System.out::println);


        sc.stop();
    }
}