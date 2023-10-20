package com.atguigu.bigdata.spark.core.rdd.action;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SparkRDD01_Action {
    public static void main(String[] args) throws Exception {

        // TODO 行动算子
        //      所谓的行动算子 : 就是通过调用RDD的方法让Spark的功能行动起来
        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Action");

        List<String> stringList = Arrays.asList("zhangsan", "lisi", "wangwu");
        final JavaRDD<String> rdd = sc.parallelize(stringList);

        // TODO 转换算子的核心作用是将RDD的功能进行补充，并不是让功能运行起来。
        //      只有调用行动算子才能让Spark的功能开始执行.
        //      行动算子会真正执行Spark的Job
        //      转换算子：返回新的RDD
        //      行动算子：返回Job的执行结果

        /*
        代码的执行位置：

            driver端 : RDD算子外部的代码都是在Driver端
                      当前程序的main方法中的代码是在Driver端。
                      main方法也称之main线程，所以Driver端代码也称之为Driver线程代码
                环境的构建
                对接数据源
                构建RDD
                数据切片
            executor端 :
                数据计算：RDD算子内部的代码
         */
        final JavaRDD<String> map = rdd.map(
                name -> {
                    System.out.println("************");
                    return "Name:" + name;
                }
        );

        final JavaRDD<String> stringJavaRDD = rdd.sortBy((s) -> {
            return s.substring(0, 1);
        }, false, 2);

        // 采集
        final List<String> collect = map.collect();
        System.out.println("***************");
        Thread.sleep(9999999);


        sc.stop();
    }
}
