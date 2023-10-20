package com.atguigu.bigdata.spark.core.rdd.action;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SparkRDD06_Action_foreach {
    public static void main(String[] args) throws Exception {

        // TODO 行动算子
        //      所谓的行动算子 : 就是通过调用RDD的方法让Spark的功能行动起来
        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Action");

        final JavaRDD<Integer> rdd = sc.parallelize(
                Arrays.asList(
                        1, 2, 3, 4
                ), 2
        );
        //rdd.collect().forEach( num -> System.out.println(num) );
        //System.out.println("***************************");
        //rdd.foreach( num -> System.out.println(num) );
        rdd.foreach(
                (num) -> {
                    System.out.println("#################");
                }
        );
        rdd.foreachPartition(
                (iter) -> {
                    System.out.println("****************");
                }
        );

        // TODO 代码的执行位置
        //    Driver端   ：算子的外部代码
        //    Executor端 ： 算子的内部代码

        sc.stop();
    }
}
