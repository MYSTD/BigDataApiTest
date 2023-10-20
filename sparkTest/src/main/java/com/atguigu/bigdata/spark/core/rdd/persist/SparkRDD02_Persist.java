package com.atguigu.bigdata.spark.core.rdd.persist;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

public class SparkRDD02_Persist {
    public static void main(String[] args) throws Exception {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Persist");

        // TODO 先完成，再完美，最后完美的完成！
        final JavaRDD<String> rdd1 = sc.textFile("data/word.txt");
        final JavaPairRDD<String, Integer> mapPairRDD1 = rdd1.mapToPair(line -> {
            System.out.println("##########");
            return new Tuple2<>(line, 1);
        });

        //final JavaPairRDD<String, Integer> cacheRDD = mapPairRDD1.cache();
        // TODO cache方法表示数据缓存
        //mapPairRDD1.cache();
        // TODO : persist方法表示数据持久化
        //mapPairRDD1.persist(StorageLevel.MEMORY_AND_DISK());
        // cache和persist只对当前的应用有效

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
