package com.atguigu.bigdata.spark.core.rdd.persist;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SparkRDD03_Persist {
    public static void main(String[] args) throws Exception {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Persist");
        sc.setCheckpointDir("cp");

        // TODO 先完成，再完美，最后完美的完成！
        final JavaRDD<String> rdd1 = sc.textFile("data/word.txt");
        final JavaPairRDD<String, Integer> mapPairRDD1 = rdd1.mapToPair(line -> {
            System.out.println("##########");
            return new Tuple2<>(line, 1);
        });

        // TODO checkpoint检查点，可以将RDD的结果长久保持下去，在多个应用中使用
        //      使用前需要配置检查点路径,默认推荐使用HDFS
        //      checkpoint检查点在执行时，为了保证程序逻辑的正确型，会在当前Job执行后，再跑一遍Job
        //      一般情况下，为了提高效率，会和cache操作联合使用
        mapPairRDD1.cache();
        mapPairRDD1.checkpoint();

        final JavaPairRDD<String, Integer> reduceRDD = mapPairRDD1.reduceByKey(Integer::sum);
        reduceRDD.collect().forEach(System.out::println);
        System.out.println("*******************************************");
        final JavaPairRDD<String, Iterable<Integer>> groupByRDD2 = mapPairRDD1.groupByKey();
        groupByRDD2.collect().forEach(System.out::println);

        // TODO 如果App流程中，多个RDD重复使用，那么可以对RDD进行优化操作
        //      将重复使用的RDD对象的计算结果进行临时的保存
        //      需要调用RDD对象的特殊的方法

        sc.stop();
    }
}
