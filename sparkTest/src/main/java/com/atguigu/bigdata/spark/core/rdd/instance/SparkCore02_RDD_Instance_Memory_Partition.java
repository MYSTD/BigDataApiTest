package com.atguigu.bigdata.spark.core.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SparkCore02_RDD_Instance_Memory_Partition {
    public static void main(String[] args) {

        // TODO 构建环境对象
        //JavaSparkContext sc = new JavaSparkContext("local[2]", "RDDInstance_Memory");
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("SparkCoreEnv");
        //conf.set("spark.default.parallelism", "4");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 1. 对接内存数据源（集合），获取RDD对象
        List<String> dataList = Arrays.asList("zhangsan", "lisi", "wangwu");
        // parallelize : 并行
        // parallelize方法可以传递2个参数：
        //    第一个参数表示内存集合对象，其实就是数据源
        //    第二个参数表示分区数量，但是可以不用传递,默认会使用当前环境中的最大虚拟核数
        //       如果配置了参数，那么默认核数不起作用
        //       scheduler.conf.getInt("spark.default.parallelism", totalCores)
        //       入口参数 > spark.default.parallelism > totalCores
        final JavaRDD<String> rdd = sc.parallelize(dataList,3);
//        final JavaRDD<String> rdd = sc.parallelize(dataList);

        // saveAsTextFile将RDD对象中流转的数据保存成分区文本文件
        rdd.saveAsTextFile("output");

        sc.stop();
    }
}
