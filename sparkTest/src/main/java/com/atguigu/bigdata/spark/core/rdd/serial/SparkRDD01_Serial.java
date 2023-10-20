package com.atguigu.bigdata.spark.core.rdd.serial;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Arrays;

public class SparkRDD01_Serial {
    public static void main(String[] args) throws Exception {

        // TODO 行动算子
        //      所谓的行动算子 : 就是通过调用RDD的方法让Spark的功能行动起来
        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Action");

        final JavaRDD<String> rdd = sc.parallelize(
                Arrays.asList("spark", "hadoop", "hive")
        );

        Search s = new Search("s");
        //s.match(rdd).collect().forEach(System.out::println);
//        s.match(rdd).foreach(System.out::println); // NotSerializableException: java.io.PrintStream
        s.match(rdd).foreach(x -> {
            System.out.println(x);
        });

        sc.stop();
    }
}
class Search {
    public static String query;
    public Search( String query ) {
        this.query = query;
    }
    public JavaRDD<String> match( JavaRDD<String> rdd ) {
        //String q = this.query;
        return rdd.filter(
                s -> s.substring(0,1).equals(query)
        );
    }
}