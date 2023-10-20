package com.atguigu.kafka.producer;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * kafka的生产者客户端开发（带回调异步发送）
 *    1. 获取客户端连接对象
 *    2. 调用API进行消息发送
 *    3. 关闭连接
 */
public class ProducerClientAsyncCallBack {
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
        for (int i = 0; i < 10; i++) {
            // 2.  调用API进行消息发送(带回调的异步发送)
            kafkaProducer.send(new ProducerRecord<>("bigdata",1,null,"atguigu" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception == null){
                        // 正常处理返回的数据
                        System.out.println("主题:" + metadata.topic() + "  分区："+ metadata.partition() + "  发送时间：" + metadata.timestamp());
                    }else {
                        exception.printStackTrace();
                    }

                }
            });

        }

        // 3. 关闭连接
        kafkaProducer.close();

    }
}