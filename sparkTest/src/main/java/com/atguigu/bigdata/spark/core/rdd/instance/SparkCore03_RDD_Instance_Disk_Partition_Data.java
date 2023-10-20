package com.atguigu.bigdata.spark.core.rdd.instance;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkCore03_RDD_Instance_Disk_Partition_Data {
    public static void main(String[] args) {

        // TODO 构建环境对象
        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDDInstance_Disk");

        // TODO 分区数据的存储和Hadoop相关
        //      Hadoop 进行切片的逻辑和读取数据进行分区保存的逻辑不一样
        //          切片的逻辑就是均分
        //               均分指的就是字节均分
        //          读取数据进行分区保存的时候不能均分
        //               hadoop是按行读取(按业务读取)，不是按照字节读取的。
        //               hadoop是按数据偏移量读取[]。
        //               hadoop数据偏移量不能重复读取。
        /*

          1@@ => 012
          2@@ => 345
          3   => 6
          ---------------------
          [0, 2]
          [2, 4]
          [4, 6]
          [6, 7]
          ---------------------
          [1]
          [2]
          [3]
          []
         */

        /*
            totalsize : 16 byte
            goalsize  : totalsize / 4 = 4 byte
            16 / 4 = 4
            ----------------------------------
            [0, 4]
            [4, 8]
            [8, 12]
            [12, 16]
            ----------------------------------
            @1111##  => 0123456
            2##      => 789
            333333   => 101112131415
            ---------------------------------
            [@1111]
            [2]
            [333333]
            []



         */
        final JavaRDD<String> rdd = sc.textFile("data/word.txt", 3);

        rdd.saveAsTextFile("output");

        sc.stop();
    }
}
