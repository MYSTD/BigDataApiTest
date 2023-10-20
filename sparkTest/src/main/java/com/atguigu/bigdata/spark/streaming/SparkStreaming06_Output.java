package com.atguigu.bigdata.spark.streaming;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class SparkStreaming06_Output {
    public static void main(String[] args) throws Exception {

        JavaStreamingContext jsc =
            new JavaStreamingContext(
                "local[*]",
                "SparkStreaming",
                Duration.apply(3000)
            );


        final JavaReceiverInputDStream<String> socketDS =
                jsc.socketTextStream("localhost", 9999);

        // TODO : No output operations registered, so nothing to execute
        //   有界数据流 & 无界数据流
        socketDS.print();
        //socketDS.mapToPair(num -> new Tuple2<>(num, 1)).saveAsHadoopFiles();
        socketDS.foreachRDD(
            rdd -> {
                rdd.collect().forEach(System.out::println);
            }
        );

        jsc.start();
        jsc.awaitTermination();
    }
}
