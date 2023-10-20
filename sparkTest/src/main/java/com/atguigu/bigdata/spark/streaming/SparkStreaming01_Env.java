package com.atguigu.bigdata.spark.streaming;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreaming01_Env {
    public static void main(String[] args) throws Exception {

        // TODO SparkStreaming是一个基于SparkCore的功能模块
        //      所以底层对SparkCore进行了封装:
        //         1. 环境 : JavaStreamingContext
        //              JavaStreamingContext对象需要传递三个参数
        //                   第三个参数表示数据的采集周期
        //         2. 模型
        JavaStreamingContext jsc =
            new JavaStreamingContext(
                "local[*]",
                "SparkStreaming",
                    //new Duration(3000)
                Duration.apply(3000)
                //Seconds.apply(3000)
            );
        // TODO SparkStreaming需要启动一个【采集器】，周期性地采集数据
        //      采集数据后封装数据源，然后和Driver进行交互，形成RDD，然后执行后续处理
        //      采集器其实可以理解为一个独立的应用程序，也可以理解为Spark的一个独立运行任务
        //      需要独立启动采集器
        jsc.start();

        //jsc.stop();
        // TODO 等待采集器结束
        jsc.awaitTermination();
    }
}
