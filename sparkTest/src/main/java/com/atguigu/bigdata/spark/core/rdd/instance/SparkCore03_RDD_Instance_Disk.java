package com.atguigu.bigdata.spark.core.rdd.instance;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SparkCore03_RDD_Instance_Disk {
    public static void main(String[] args) {

        // TODO 构建环境对象
        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDDInstance_Disk");

        // 1. 对接磁盘文件数据源，获取RDD对象
        //    textFile方法可以将文件数据和RDD对象进行对接
        //        方法需要传递参数，表示文件路径（绝对路径，相对路径）
        //final JavaRDD<String> rdd = sc.textFile("D:\\idea\\classes\\classes-bigdata-bj230529\\data\\word.txt");
        //    相对路径以当前的项目的根路径为基准
        //final JavaRDD<String> rdd = sc.textFile("data/word.txt,data/word1.txt,test.txt");
        //final JavaRDD<String> rdd = sc.textFile("data");
        final JavaRDD<String> rdd = sc.textFile("data/word*.txt");

        rdd.collect().forEach(System.out::println);

        sc.stop();
    }
}
