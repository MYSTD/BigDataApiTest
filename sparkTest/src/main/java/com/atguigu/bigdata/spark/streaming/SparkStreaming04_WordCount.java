package com.atguigu.bigdata.spark.streaming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;

public class SparkStreaming04_WordCount {
    public static void main(String[] args) throws Exception {

        JavaStreamingContext jsc =
            new JavaStreamingContext(
                "local[*]",
                "SparkStreaming",
                Duration.apply(3000)

            );

        // TODO 从Socket中获取数据，统计Wordcount
        final JavaReceiverInputDStream<String> socketDS =
                jsc.socketTextStream("localhost", 9999);

        // TODO 获取的数据也是一行一行的。
        final JavaPairDStream<String, Integer> pairDS = socketDS.mapToPair(
                word -> new Tuple2<>(word, 1)
        );

        // TODO SparkStreaming默认的统计以一个采集周期为单位
        pairDS.reduceByKey(Integer::sum).print();

        jsc.start();
        jsc.awaitTermination();
    }
}
