package com.atguigu.bigdata.spark.core;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkTest1 {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "Test");

        // TODO 读取json文件，计算用户年龄的平均值
        //     Spark读取json文件时，一般采用一行数据就是一个json对象的方式

        // TODO Spark Core的核心作用是利用RDD进行分布式计算，而不是解析不同格式的数据
        //      所以在特殊格式的数据文件处理中，Spark Core是不适用的，此时可以采用其他的功能模块进行简化和优化
        //      Spark SQL就是专门用于处理特殊数据格式（结构化数据）的功能模块
        final JavaRDD<String> jsonRDD = sc.textFile("data/user.json");

        jsonRDD.filter(
                line -> {
                    final String[] split = line.split(":");
                    if ( split.length == 2 ) {
                        final String attr = split[0];
                        return "\"age\"".equals(attr.trim());
                    } else {
                        return false;
                    }
                }
        )
        .map(
                line -> {
                    final String[] split = line.split(":");
                    final String s = split[1];
                    final String trim = s.trim();
                    if ( trim.endsWith(",") ) {
                        return Integer.parseInt(s.trim().substring(0, s.trim().length()-1).trim());
                    } else {
                        return Integer.parseInt( trim );
                    }
                }
        )
        .collect().forEach(System.out::println);



        sc.stop();
    }
}
