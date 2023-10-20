package com.atguigu.bigdata.spark.core.rdd.bc;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

public class SparkRDD01_Bc {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Transform_groupBy");
        final JavaRDD<String> rdd = sc.parallelize(Arrays.asList(
                "hello", "spark", "hive", "flume", "flink", "hadoop"
        ), 3);

        // TODO 算子内部的代码逻辑是再Executor端执行的
        // TODO 算子外部的代码逻辑是在Driver端执行的
        //      如果算子的内部代码使用了外部代码中的对象，那么对象需要进行序列化在网络中传递的
        //      对象传递的次数以Task为单位
        //       Executor计算节点的内部存在线程池，也就意味着可以同时执行多个Task
        //      那么driver端数据传递给Task执行的时候，就可能出现大量的数据冗余，会对象性能造成影响。
        //      优化策略：更改数据传输方式，以Executor为单位进行数据传输
        //              RDD数据模型做不到，需要采用其他数据模型(广播变量)进行处理：
        //                 1. 在Driver端将数据进行包装，形成广播变量
        //                 2. 在Executor端将数据进行获取
        List<String> ss = Arrays.asList("s", "f");
        final Broadcast<List<String>> broadcast = sc.broadcast(ss);

        rdd.filter(
                s -> {
                    return broadcast.value().contains(s.substring(0, 1));
                }
        ).collect().forEach(System.out::println);




        sc.stop();
    }
}