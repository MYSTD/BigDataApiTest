package com.atguigu.bigdata.spark.core.rdd.serial;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Arrays;

public class SparkRDD02_Serial {
    public static void main(String[] args) throws Exception {

        // TODO 行动算子
        //      所谓的行动算子 : 就是通过调用RDD的方法让Spark的功能行动起来
        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Action");

        final JavaRDD<Integer> rdd = sc.parallelize(
                Arrays.asList(1, 2, 3, 4),2
        );

        Student s = new Student();

        // TODO RDD算子内部的代码逻辑中如果使用了算子外部的对象，那么对象需要通过网络传递到Executor端
        //      所以传输的对象必须要能够序列化。
        rdd.foreach(
                num -> {
                    s.sum = s.sum + num;
                }
        );
        System.out.println(s.sum);


        sc.stop();
    }
}
class Student implements Serializable {
    public int sum;
}