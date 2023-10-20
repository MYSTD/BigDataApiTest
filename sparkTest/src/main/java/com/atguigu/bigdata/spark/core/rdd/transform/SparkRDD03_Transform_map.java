package com.atguigu.bigdata.spark.core.rdd.transform;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;

public class SparkRDD03_Transform_map {
    public static void main(String[] args) throws Exception {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "RDD_Transform_map");

        // TODO 对接内存数据源
        final JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);

        // TODO 补充新的功能 : num * 2
        //      map方法的作用：转换，映射,(定位)
        //         HashMap : k -> v
        final JavaRDD<Integer> mapRDD1 = rdd.map(new MyFunction1());
        final JavaRDD<Integer> mapRDD2 = mapRDD1.map(new MyFunction2());

        mapRDD2.collect().forEach(System.out::println);
        //mapRDD2.saveAsTextFile("output");
        Thread.sleep(9999999999L);

        sc.stop();
    }
}
class MyFunction1 implements Function<Integer, Integer> {
    @Override
    public Integer call(Integer v) throws Exception {
        System.out.println("### = " + v);
        return v;
    }
}
class MyFunction2 implements Function<Integer, Integer> {
    @Override
    public Integer call(Integer v) throws Exception {
        System.out.println("%%% = " + v);
        return v;
    }
}
