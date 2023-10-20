package com.atguigu.bigdata.spark.core.rdd.action;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class SparkRDD05_Action_Save {
    public static void main(String[] args) throws Exception {

        // TODO 行动算子
        //      所谓的行动算子 : 就是通过调用RDD的方法让Spark的功能行动起来
        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Action");

        final JavaRDD<Integer> rdd = sc.parallelize(
                Arrays.asList(
                        1, 2, 3, 4
                ), 2
        );

        // TODO 文件路径其实是基于当前环境的
        //     Application : 应用程序
        //     Job的数量 ：只要执行一个行动算子，就会产生一个作业
        rdd.saveAsTextFile("output");
        rdd.saveAsObjectFile("output1");

        sc.stop();
    }
}
