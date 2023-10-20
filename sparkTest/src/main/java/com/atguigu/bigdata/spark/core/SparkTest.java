package com.atguigu.bigdata.spark.core;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.Optional;

public class SparkTest {
    public static void main(String[] args) {

        // TODO csv文件一种特殊格式的数据文件：以逗号分隔数据的格式
        // TODO tsv文件一种特殊格式的数据文件：以\t分隔数据的格式
        // TODO json文件一种特殊格式的数据文件：
        //      JSON : JavaScript Object Notation
        JavaSparkContext sc = new JavaSparkContext("local[*]", "Test");

        // TODO 读取csv文件，计算用户年龄的平均值
        final JavaRDD<String> csvRDD = sc.textFile("data/user.csv");
        // zhangsan,30,男
        /*
        30, 40, 50
         */
        final JavaRDD<Integer> ageRDD = csvRDD.map(
                line -> {
                    final String[] data = line.split(",");
                    return Integer.parseInt(data[1]);
                }
        );


        // total age / user count
//        final Integer totalAge = ageRDD.collect().stream().reduce(Integer::sum).get();
//        final long count = ageRDD.count();
//        long avgAge = totalAge / count;
//        System.out.println(avgAge);

//        final JavaPairRDD<String, Iterable<Integer>> groupRDD = ageRDD.groupBy(num -> "age");
//        groupRDD.mapValues(
//                iter -> {
//
//                }
//        )

//        final List<Integer> collect = ageRDD.collect();
//        collect.sum() / collect.size();


        sc.stop();
    }
}
