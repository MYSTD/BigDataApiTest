package com.atguigu.bigdata.spark.core.rdd.transform;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SparkRDD01_Transform {
    public static void main(String[] args) {

        // TODO 转换算子
        //      所谓的转换算子，其实就是通过RDD对象方法将一个RDD对象转换成一个新的RDD对象
        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Transform");

        List<String> stringList = Arrays.asList("zhangsan", "lisi", "wangwu");

        // InputStream in = new FileInputStream()
        // TODO ParallelCollectionRDD : 对接内存数据源
        final JavaRDD<String> rdd = sc.parallelize(stringList);
        // TODO HadoopRDD : 对接磁盘文件数据源
        //final JavaRDD<String> rdd = sc.textFile("data/word.txt");

        // TODO 希望将多个RDD的功能组合在一起实现复杂的逻辑
        // zhangsan => Name:zhangsan
        // lisi => Name:lisi
        // wangwu => Name:wangwu
//        rdd.collect().forEach(
//            ( name ) -> {
//                System.out.println("Name : " + name);
//            }
//        );
        // TODO 将多个RDD功能组合在一起，框架采用的是算子（方法）的方式
        //  map方法就是转换算子
        final JavaRDD<Object> map = rdd.map(null);

        sc.stop();
    }
}
