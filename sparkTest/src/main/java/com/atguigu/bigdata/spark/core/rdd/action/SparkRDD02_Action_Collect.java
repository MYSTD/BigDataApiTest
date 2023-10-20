package com.atguigu.bigdata.spark.core.rdd.action;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SparkRDD02_Action_Collect {
    public static void main(String[] args) throws Exception {

        // TODO 行动算子
        //      所谓的行动算子 : 就是通过调用RDD的方法让Spark的功能行动起来
        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Action");

        List<String> stringList = Arrays.asList("wangwu", "zhaoliu", "zhangsan", "lisi");
        final JavaRDD<String> rdd = sc.parallelize(stringList, 2);

        // TODO collect方法就是行动算子，会让Spark的Job开始执行
        //      collect方法的作用是将不同Executor的结果数据采集回到Driver端，并且按照分区顺序进行采集
        final List<String> collect = rdd.collect();
        collect.forEach(System.out::println);


        sc.stop();
    }
}
