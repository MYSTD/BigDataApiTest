package com.atguigu.bigdata.spark.core.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SparkCore02_RDD_Instance_Memory_Partition_Data {
    public static void main(String[] args) {

        // TODO 构建环境对象
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("SparkCoreEnv");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> dataList = Arrays.asList("zhangsan", "lisi", "wangwu", "zhaoliu", "tianqi", "aaaa");
        /*
        -----------------------
        4
        【zhangsan】
        【lisi】
        【wangwu，zhaoliu】
        ----------------------
        5
        【zhangsan】
        【lisi, wangwu】
        【zhaoliu,tianqi】


         */
        final JavaRDD<String> rdd = sc.parallelize(dataList, 4);

        // saveAsTextFile将RDD对象中流转的数据保存成分区文本文件
        rdd.saveAsTextFile("output");

        sc.stop();
    }
}
