package com.atguigu.bigdata.spark.streaming;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class SparkStreaming05_Window {
    public static void main(String[] args) throws Exception {

        JavaStreamingContext jsc =
            new JavaStreamingContext(
                "local[*]",
                "SparkStreaming",
                Duration.apply(3000)

            );

        // TODO 获取最近1小时每10分钟的气温变化趋势
        //      SparkStreaming可以将一个范围中的数据采集后，再进行计算，这个采集的范围，称之为窗口
        //      采集范围和采集周期不是一回事。采集范围应该是采集周期的正数倍
        final JavaReceiverInputDStream<String> socketDS =
                jsc.socketTextStream("localhost", 9999);

        // TODO window方法就表示窗口，方法会传递两个参数
        //      第一个参数表示窗口的范围大小，应该是采集周期的正数倍
        //      第二个参数表示窗口的滑动幅度，应该是采集周期的正数倍
        //     SparkStreaming数据计算时间点为滑动时间点。
//        final JavaDStream<String> window =
//                socketDS.window(Duration.apply(3000), Duration.apply(6000));

        // TODO 窗口的处理过程中，根据窗口的滑动幅度不同，存在不同的类型
        //final JavaPairDStream<String, Integer> reduceDS = window.mapToPair(num -> new Tuple2<>(num, 1)).reduceByKey(Integer::sum);

        // TODO reduceByKeyAndWindow = reduceByKey + window
        //socketDS.mapToPair(num -> new Tuple2<>(num, 1)).reduceByKeyAndWindow(Integer::sum, Duration.apply(3000), Duration.apply(6000));

        //reduceDS.print();


        jsc.start();
        jsc.awaitTermination();
    }
}
