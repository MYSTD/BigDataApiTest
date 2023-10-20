package com.atguigu.bigdata.spark.core.rdd.action;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SparkRDD03_Action_Count {
    public static void main(String[] args) throws Exception {

        // TODO 行动算子
        //      所谓的行动算子 : 就是通过调用RDD的方法让Spark的功能行动起来
        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Action");

        List<String> stringList = Arrays.asList("wangwu", "zhaoliu", "zhangsan", "lisi");
        final JavaRDD<String> rdd = sc.parallelize(stringList, 2);
        final long count = rdd.count();
        final String first = rdd.first();
        System.out.println(count);


        sc.stop();
    }
}
