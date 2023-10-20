package com.atguigu.bigdata.spark.core.rdd.part;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkRDD03_Part_DIY {
    public static void main(String[] args) throws Exception {

        // TODO 自定义分区器的编码顺序
        //      1. 创建自定义分区器类，继承Partitioner
        //      2. 重写方法 4个方法
        //         2.1 numPartitions方法 : 分区数量的设定s
        //         2.2 getPartition方法 : 通过数据的key计算出数据存储的分区编号（从0开始）
        //         2.3 equals方法 : 判断分区器是否是同一个。如果分区器相同，底层实现时，会有特殊处理
        //         2.4 hashCode方法 : 一般重写equals方法，都会重写hashCode方法
        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Transform_groupBy");
        final JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(
                Arrays.asList(
                        new Tuple2<>("a", 1),
                        new Tuple2<>("a", 2),
                        new Tuple2<>("a", 3),
                        new Tuple2<>("a", 4)
                ),2
        );
        System.out.println(rdd.partitioner());
        final JavaPairRDD<String, Integer> reduceRDD = rdd.reduceByKey(Integer::sum);
        System.out.println(reduceRDD.partitioner());
        final JavaPairRDD<String, Integer> reduceRDD1 = reduceRDD.reduceByKey(
                new HashPartitioner(4),
                Integer::sum);
        System.out.println(reduceRDD1.partitioner());
        reduceRDD1.collect().forEach(System.out::println);//saveAsTextFile("output");
        // 分区数量？
        // 阶段数量？ 1(ResultStage) + 宽依赖的数量(ShuffleMapStage) = 1 + 2 = 3
        // 任务数量？ 2 + 2 + 2 = 6
        Thread.sleep(9999999);


        sc.stop();
    }
}