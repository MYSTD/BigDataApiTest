package com.atguigu.bigdata.spark.core.rdd.persist;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SparkRDD04_Persist {
    public static void main(String[] args) throws Exception {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Persist");
        sc.setCheckpointDir("cp");

        // TODO 先完成，再完美，最后完美的完成！
        final JavaRDD<String> rdd1 = sc.textFile("data/word.txt");
        final JavaPairRDD<String, Integer> mapPairRDD1 = rdd1.mapToPair(line -> {
            //System.out.println("##########");
            return new Tuple2<>(line, 1);
        });

        // TODO cache方法不会切断血缘，而是再血缘中增加新的依赖关系
        //      checkpoint方法会切断血缘
        System.out.println(mapPairRDD1.toDebugString());
        System.out.println("*************************");
        //mapPairRDD1.cache();
        mapPairRDD1.checkpoint();
        System.out.println(mapPairRDD1.toDebugString());
        System.out.println("*************************");
        //mapPairRDD1.checkpoint();

        final JavaPairRDD<String, Integer> reduceRDD = mapPairRDD1.reduceByKey(Integer::sum);
        System.out.println(reduceRDD.toDebugString());
        System.out.println("*************************");
        reduceRDD.collect();//.forEach(System.out::println);
        System.out.println(reduceRDD.toDebugString());
        System.out.println("*************************");

        sc.stop();
    }
}
