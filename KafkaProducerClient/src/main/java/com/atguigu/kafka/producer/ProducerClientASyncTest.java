package com.atguigu.kafka.producer;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * kafka的生产者客户端开发（普通异步发送）
 *    1. 获取客户端连接对象
 *    2. 调用API进行消息发送
 *    3. 关闭连接
 */
public class ProducerClientASyncTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 0. 声明一个配置对象
        Properties properties = new Properties();
        // -- 指定Kafka的Borker端的地址
//        properties.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");
        // -- 给消息的key和value指定序列化器
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // 1. 获取客户端连接对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        try {
            for (int i = 0; i < 100; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>("your-topic", "key-" + i, "value-" + i);
                kafkaProducer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            System.out.println("Message with key " + record.key() + " sent to partition " + metadata.partition() + " with offset " + metadata.offset());
                        } else {
                            exception.printStackTrace();
                        }
                    }
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.close();
        }
    }
}