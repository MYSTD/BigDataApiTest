package com.atguigu.bigdata.spark.core.rdd.transform;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SparkRDD08_Transform_sortBy {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Transform_groupBy");
        final JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(
                1,4,3,6,5,2
        ), 3);

        // TODO sortBy方法是将数据按照指定的规则进行排序
        //      sortBy方法需要传递三个参数
        //          第一个参数为函数表达式： Data -> 排序的规则
        //               实际的排序是以表达式的结果排序
        /*
            1   6   3   5  4  2
            0   0   0   0  0  0
            -------------------
            1  11  2   22  3  4
            "1"  "11"  "2"   "22"  "3"  "4"
            -------------------
            1   11  2  22  3  4
            --------------------
            1,   4,   3,   6,   5,  2
            1    0    1    0    1   0



         */
        //          第二个参数表示排序的方式：升序（true），降序(false)
        //          第三个参数表示排序的分区数量：方法底层是靠shuffle实现的
        final JavaRDD<Integer> sortByRDD = rdd.sortBy(num -> num % 2, true, 2);

        sortByRDD.collect().forEach(System.out::println);



        sc.stop();
    }
}