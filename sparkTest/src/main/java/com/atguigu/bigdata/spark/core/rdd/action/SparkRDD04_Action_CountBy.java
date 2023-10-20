package com.atguigu.bigdata.spark.core.rdd.action;

import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class SparkRDD04_Action_CountBy {
    public static void main(String[] args) throws Exception {

        // TODO 行动算子
        //      所谓的行动算子 : 就是通过调用RDD的方法让Spark的功能行动起来
        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Action");

        // TODO countByKey : 这里的key表示的是键值数据中的key的意思
        // TODO countByValue : 这里的Value表示的是值的意思
        //                     表示数据源中相同的value（值）有多少个
        final Map<String, Long> stringLongMap = sc.parallelizePairs(
                Arrays.asList(
                        new Tuple2<>("a", 1),
                        new Tuple2<>("a", 2),
                        new Tuple2<>("a", 3),
                        new Tuple2<>("a", 4)
                ), 2
        ).countByKey();

        System.out.println(stringLongMap);
        sc.stop();
    }
}
