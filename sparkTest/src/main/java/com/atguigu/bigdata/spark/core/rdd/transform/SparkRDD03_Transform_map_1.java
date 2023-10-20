package com.atguigu.bigdata.spark.core.rdd.transform;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

public class SparkRDD03_Transform_map_1 {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Transform_map");

        // TODO 对接内存数据源
        final JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);

        // TODO 补充新的功能 : num * 2
        //      map方法的作用：转换，映射,(定位)
        //         HashMap : k -> v
        final JavaRDD<Integer> mapRDD1 = rdd.map(
                MyCalc::multi
        );
        final JavaRDD<String> mapRDD2 = mapRDD1.map(
                num -> "abc"
        );

        //mapRDD.collect().forEach(System.out::println);
        mapRDD2.saveAsTextFile("output");

        sc.stop();
    }
}
class MyCalc {
    public static Integer multi( Integer num ) {
        return num * 2;
    }
}