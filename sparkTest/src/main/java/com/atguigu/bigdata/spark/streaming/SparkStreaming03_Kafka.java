package com.atguigu.bigdata.spark.streaming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;

public class SparkStreaming03_Kafka {
    public static void main(String[] args) throws Exception {

        JavaStreamingContext jsc =
            new JavaStreamingContext(
                "local[*]",
                "SparkStreaming",
                Duration.apply(3000)

            );

        // 创建配置参数
        HashMap<String, Object> paramMap = new HashMap<>();
        paramMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"linux1:9092,linux2:9092,linux3:9092");
        paramMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        paramMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        paramMap.put(ConsumerConfig.GROUP_ID_CONFIG,"atguigu");
        paramMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        // 需要消费的主题
        ArrayList<String> topics = new ArrayList<>();
        topics.add("topic_db");

        // TODO 对接Kafka数据源
        JavaInputDStream<ConsumerRecord<String, String>> kafkaDS =
                KafkaUtils.createDirectStream(
                        jsc,
                        LocationStrategies.PreferBrokers(),
                        ConsumerStrategies.<String, String>Subscribe(topics, paramMap));

        final JavaDStream<String> map = kafkaDS.map(
                record -> record.value()
        );
        map.print();

        jsc.start();
        jsc.awaitTermination();
    }
}
