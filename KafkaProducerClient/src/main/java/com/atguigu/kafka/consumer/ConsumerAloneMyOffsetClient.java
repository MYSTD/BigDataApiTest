package com.atguigu.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/*
* TODO 消费者消费消息的方式
*      -- 从头消费
*      -- 从自己维护的最新的offset进行消费
*      -- 有业务需求决定 指定offset消费（不常用...）
* */
public class ConsumerAloneMyOffsetClient {

    public static void main(String[] args) {

        // 1. 声明配置文件
        Properties properties = new Properties();
        // 1.1 指定Kafka的地址
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop104:9092");
        // 1.2 指定消费者端的反序列化器
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 1.3 指定消费者组
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"group07");
        // 1.4 指定当前消费者从头消费
//        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
//        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"none");
        // 2. 获取客户端连接对象
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
       /* ArrayList<String> topics = new ArrayList<>();
        topics.add("first");
        // 2.1 订阅主题
        kafkaConsumer.subscribe(topics);*/
        // 声明装载分区集合
        ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
        TopicPartition first0 = new TopicPartition("first", 0);
        TopicPartition first1 = new TopicPartition("first", 1);
        TopicPartition first2 = new TopicPartition("first", 2);
        topicPartitions.add(first0);
        topicPartitions.add(first1);
        topicPartitions.add(first2);
        kafkaConsumer.assign(topicPartitions);
        // 指定特定分区的offset
        kafkaConsumer.seek(first0,5000);
        kafkaConsumer.seek(first1,5010);
        kafkaConsumer.seek(first2,10);
        // 3. 调用消费者API执行操作
        while (true){
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            // 处理数据 。。。。
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }

        }
    }
}
