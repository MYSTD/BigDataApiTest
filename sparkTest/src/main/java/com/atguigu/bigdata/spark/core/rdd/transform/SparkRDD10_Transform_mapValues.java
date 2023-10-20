package com.atguigu.bigdata.spark.core.rdd.transform;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkRDD10_Transform_mapValues {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Transform_groupBy");

        /*
          (a, 1) =>(a, 2)
          (b, 2) =>(b, 4)
          (c, 3) =>(c, 6)
         */

        // TODO mapValues方法：
        //     Spark中处理的数据如果是k,v类型的时候，一般会将k和v当成两个不同的东西来考虑
        //     mapValues方法表示进行转换时，k不进行转换，只对v进行转换
//        final JavaRDD<Tuple2<String, Integer>> rdd1 = sc.parallelize(
//                Arrays.asList(
//                        new Tuple2<>("a", 1),
//                        new Tuple2<>("b", 2),
//                        new Tuple2<>("c", 3)
//                )
//        );
//        rdd1.map(
//                ( t ) -> new Tuple2<>( t._1, t._2 * 2 )
//        ).collect().forEach(System.out::println);

        final JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(
                Arrays.asList(
                        new Tuple2<>("a", 1),
                        new Tuple2<>("b", 2),
                        new Tuple2<>("c", 3)
                )
        );

        rdd.mapValues(v->v * 2).collect().forEach(System.out::println);

        /*
           groupBy

           ["hadoop", 【hadoop,hadoop,hadoop】]
           ["spark",  【spark,spark,spark】]

           mapValues

           ["hadoop", 【hadoop,hadoop,hadoop】] => (hadoop, 3)
           ["spark",  【spark,spark,spark】]    => (spark,  3)


         */


        sc.stop();
    }
}