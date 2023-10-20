package com.atguigu.bigdata.spark.core.rdd.transform;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.List;

public class SparkRDD02_Transform_DataType {
    public static void main(String[] args) {

        // TODO 将无关的数据当成一个整体来使用的时候，需要采用特殊的数据类型
        //      元组：TupleNum,默认情况下，语言提供的元素组合的个数最多为22个
        //      获取元组中的数据，一般可以采用顺序号：t._1, t._2....
        //      Scala语言的元组可以简化： (start, end)
        //           如果一个元组中的元素只有2个，一般称之为对偶元组，也称之为键值对 （k,v）
        final Tuple2<String, Integer> t2 = new Tuple2<>("zhangsan", 100);
        // Spark框架是基于Scala语言开发的，所以在数据处理上区分不同的数据类型
        // 一般情况下，Spark处理数据分为2种方式
        //   1. 普通数据：单一数据， zhangsan,1,new User(), new ArrayList()
        //          单值数据（Value）
        //   2. 特殊数据：键值数据 （k, v） -> Tuple2
    }
}
