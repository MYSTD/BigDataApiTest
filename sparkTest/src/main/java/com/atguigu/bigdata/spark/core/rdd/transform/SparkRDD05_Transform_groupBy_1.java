package com.atguigu.bigdata.spark.core.rdd.transform;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SparkRDD05_Transform_groupBy_1 {
    public static void main(String[] args) throws Exception {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Transform_groupBy");

        // TODO 对接内存数据源
        final JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(
                1,2,3,4
        ), 2);

        // groupBy方法等同于给数据增加分组标记，相同的标记在处理时会放置在一起，就是分在一个组中。
        rdd.groupBy(
                //num -> num % 2 == 0
                num -> num % 2
        )
        .collect()
        .forEach(System.out::println);

        // 使用UI界面查看当前Spark程序运行的情况
        // http://localhost:4040
        //Thread.sleep(999999999);

        sc.stop();
    }
}