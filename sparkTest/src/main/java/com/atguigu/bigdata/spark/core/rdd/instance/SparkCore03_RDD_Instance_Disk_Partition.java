package com.atguigu.bigdata.spark.core.rdd.instance;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkCore03_RDD_Instance_Disk_Partition {
    public static void main(String[] args) {

        // TODO 构建环境对象
        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDDInstance_Disk");

        // 1. 对接磁盘文件数据源，获取RDD对象
        //    textFile方法可以传递两个参数
        //        第一个参数表示文件路径
        //        第二个参数表示文件最小分区数
        //              入口参数 > math.min(defaultParallelism, 2)
        //final JavaRDD<String> rdd = sc.textFile("data/word.txt");
        //  Spark是基于MapReduce开发的。
        //  Spark是无法读取文件的。底层读文件采用的是Hadoop方式，所以文件的分区其实不是Spark决定的
        //  是Hadoop决定的，Hadoop读取文件的切片就是Spark中的分区

        //  读取文件进行切片（分区）的核心目的就是均衡
        /*
            totalsize : 7 byte
            goalsize  : totalsize / 3 = 2
            partnum = 7 / 2 = 3...1

            2G文件 / 2 => 128M
         */
        final JavaRDD<String> rdd = sc.textFile("data/word.txt", 3);

        rdd.saveAsTextFile("output");

        sc.stop();
    }
}
