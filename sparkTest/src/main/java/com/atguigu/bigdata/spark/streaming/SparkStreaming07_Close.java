package com.atguigu.bigdata.spark.streaming;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreaming07_Close {
    public static void main(String[] args) throws Exception {
        JavaStreamingContext jsc =
            new JavaStreamingContext(
                "local[*]",
                "SparkStreaming",
                Duration.apply(3000)
            );

        //jsc.stop();
        final JavaReceiverInputDStream<String> socketDS =
                jsc.socketTextStream("localhost", 9999);
        socketDS.print();

        // TODO 采集器原则上来讲不应该停止运行的。
        // TODO 但是再特殊场景中，需要停止后，重新启动，如业务升级
        //      如果想要停止采集器，可以调用环境对象stop方法
        //      一般不会再main方法（main线程）完成调用，需要创建新的线程完成调用

        //jsc.stop();
        jsc.start();


        new Thread(new Runnable() {
            @Override
            public void run() {
                // TODO 根据实际的业务场景，决定是否调用stop方法
                //      决定是否调用方法的方式：采用中间件(第三方)

                try {
                    Thread.sleep(60 * 1000);
                    //    File :
                    //    MySQL :
                    //    HDFS :
                    //    ZK :
                    //    Redis :
                } catch ( Exception e) {

                }
                // TODO 强制关闭
                //jsc.stop();
                // TODo 优雅地关闭
                jsc.stop(true, true);

            }
        }).start();
        // .......
        //jsc.stop();
        jsc.awaitTermination();
        //jsc.stop();
    }
}
