package com.atguigu.bigdata.spark.core.rdd.action;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SparkRDD03_Action_Take {
    public static void main(String[] args) throws Exception {

        // TODO 行动算子
        //      所谓的行动算子 : 就是通过调用RDD的方法让Spark的功能行动起来
        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Action");

        //List<String> stringList = Arrays.asList("wangwu", "zhaoliu", "zhangsan", "lisi");
        //final JavaRDD<String> rdd = sc.parallelize(stringList, 2);

        // TODO take方法可以获取前3条数据
        //      TopN问题
//        final List<String> take = rdd.take(3);
//        System.out.println(take);

        // 1,2,3,4,5,6 => 1,2,3
        // 1,4,3 => 1,3,4
        sc.parallelize(
                Arrays.asList(
                        1,4,3,6,2,5
                ),2
        ).takeOrdered(3)
        .forEach(System.out::println);
        sc.stop();
    }
}
