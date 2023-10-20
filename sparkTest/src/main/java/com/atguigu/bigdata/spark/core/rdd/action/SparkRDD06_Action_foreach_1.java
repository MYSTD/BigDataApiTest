package com.atguigu.bigdata.spark.core.rdd.action;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Arrays;

public class SparkRDD06_Action_foreach_1 {
    public static void main(String[] args) throws Exception {

        // TODO 行动算子
        //      所谓的行动算子 : 就是通过调用RDD的方法让Spark的功能行动起来
        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Action");

        final JavaRDD<Integer> rdd = sc.parallelize(
                Arrays.asList(
                        1, 2, 3, 4
                ), 2
        );

        User user = new User();
        rdd.foreach(
            num -> {
                System.out.println(user.toString() + num);
            }
        );

        sc.stop();
    }
}
class User implements Serializable {
    public int age = 30;
}
