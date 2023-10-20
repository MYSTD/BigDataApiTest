package com.atguigu.bigdata.spark.core.rdd.transform;

import org.apache.commons.collections4.iterators.ArrayIterator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Arrays;

public class SparkRDD04_Transform_flatMap {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Transform_flatMap");

        // TODO 对接内存数据源
        final JavaRDD<String> rdd = sc.parallelize(Arrays.asList(
                "hello_world",
                "hadoop_hive",
                "spark"
        ), 2);

        // TODO map方法的作用：A(String) -> B(String[])
        //          map方法只能做到：1个输入 -> 1个输出
        //          无法做到：1个输入 -> N个输出
        // 如果一个整体数据需要拆分成多个个体使用，那么一般称之为扁平化操作
        // flatMap方法其实就是 ：扁平化操作 + 映射转换操作
        final JavaRDD<String> flatMapRDD = rdd.flatMap(
                (s) -> {
                    final String[] ss = s.split("_");
                    // Array => Iterator
                    return Arrays.asList(ss).iterator();
//                    return new ArrayList<String>().iterator();
                    //return new ArrayIterator<String>(ss);
                }
        );

        flatMapRDD.collect().forEach(System.out::println);


        sc.stop();
    }
}