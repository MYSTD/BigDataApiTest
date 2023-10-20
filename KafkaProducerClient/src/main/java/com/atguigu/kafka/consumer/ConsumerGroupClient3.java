package com.atguigu.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/*
* TODO 消费者组案例 消费者3
* */
public class ConsumerGroupClient3 {

    public static void main(String[] args) {

        // 1. 声明配置文件
        Properties properties = new Properties();
        // 1.1 指定Kafka的地址
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop104:9092");
        // 1.2 指定消费者端的反序列化器
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 1.3 指定消费者组
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"group02");
        // 1.4 指定分区消费策略 RoundRobinAssignor
//        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());
        // 1.5 指定分区消费策略为 StickyAssignor （黏性）
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StickyAssignor.class.getName());
        // 2. 获取客户端连接对象
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        ArrayList<String> topics = new ArrayList<>();
        topics.add("bigdata");
        // 2.1 订阅主题
        kafkaConsumer.subscribe(topics);
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
