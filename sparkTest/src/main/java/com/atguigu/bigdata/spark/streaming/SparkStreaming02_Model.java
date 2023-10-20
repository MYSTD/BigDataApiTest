package com.atguigu.bigdata.spark.streaming;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreaming02_Model {
    public static void main(String[] args) throws Exception {

        // TODO SparkStreaming是一个基于SparkCore的功能模块
        //      所以底层对SparkCore进行了封装:
        //         1. 环境 : JavaStreamingContext
        //              JavaStreamingContext对象需要传递三个参数
        //                   第三个参数表示数据的采集周期
        //         2. 模型 : SparkStreaming对RDD对象也进行了封装，形成了新的数据模型
        JavaStreamingContext jsc =
            new JavaStreamingContext(
                "local[*]",
                "SparkStreaming",
                Duration.apply(3000)

            );

        // TODO 对接数据源，构建新的数据模型
        //      DStream : 对RDD的封装，底层其实就是RDD
        //                离散化流对象 => 不连续的流对象
        //      [1,2,3],[4,5,6]

        //     当前数据处理中，一般不会采用socket接收数据，一般采用kafka来接收数据
        final JavaReceiverInputDStream<String> dstream =
                jsc.socketTextStream("localhost", 9999);

        dstream.print();

        jsc.start();
        jsc.awaitTermination();
    }
}
