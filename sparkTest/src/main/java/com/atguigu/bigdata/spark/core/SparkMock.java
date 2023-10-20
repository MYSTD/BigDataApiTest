package com.atguigu.bigdata.spark.core;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkMock {
    public static void main(String[] args) {

        // TODO Top10热门品类
        //      Top10 : 取满足条件数据的前10名(排序)
        //      热门   : 点击数 >下单数 > 支付数
        //      品类   : 商品分类，相同商品分类ID是相同的。点击，下单，支付行为都会记录品类ID

        //  实现逻辑：
        //     1. 统计品类点击数 ： word(品类) + count(点击数)
        //     2. 统计品类下单数 ： word(品类) + count(下单数)
        //     3. 统计品类支付数 ： word(品类) + count(支付数)
        //    1-3 : 同时统计一个品类的多个数量： ( 品类，（点击数， 下单数， 支付数） )
        //     4. 排序
        //     5. Top10

        // List(zhangsan-30, lisi-30), List(zhangsan-3000, lisi-5000)

        // TODO 基础（代码）设计
        //  1. 对接数据源：数据文件
        //  2. 过滤文件中的数据，保留点击行为，下单行为，支付行为的数据，丢弃查询行为
        //  2.5 将行为数据进行格式转换
        //  3. 将数据进行汇总统计
        //     3.1 分组 ：将一个品类的行为数据全部放置在一起（分组）
        //          1, 点击    0       0
        //          1, 0      下单     0
        //          1, 0               支付
        //          1, 点击
        //          1, 0      下单
        //          1, 0               支付
        //          1, 点击
        //          1, 0     下单
        //          1, 0               支付
        //     3.2 聚合 ： 将一个组内的数据按照指定的规则进行聚合
        //          (1, (3, 3, 3))
        //  4. 将分组聚合后的数据根据数量进行排序（点击数 >下单数 > 支付数）
        //  5. 将排序后的数据取前10个。
        JavaSparkContext sc = new JavaSparkContext("local[*]", "Mock");

        final JavaRDD<String> fileRDD = sc.textFile("data/user_visit_action.txt");

        // filter方法需要传递一个参数（一行行为数据）进来，返回布尔值，根据布尔值决定数据是否保留
        final JavaRDD<String> filterRDD = fileRDD.filter(
                line -> {
                    final String[] datas = line.split("_");
                    String query = datas[5];
                    return "null".equals(query);
                }
        );
        //filterRDD.take(3).forEach(System.out::println);
        // (品类，（点击数，下单数，支付数）)
        // new Tuple2<>(1, new Tuple3<>(1, 0, 0))
        // new Tuple2<>(1, new Tuple3<>(0, 1, 0))
        // new Tuple2<>(1, new Tuple3<>(0, 0, 1))
        // 元组
        final JavaRDD<CategoryCount> flatMapRDD = filterRDD.flatMap(
                line -> {
                    final String[] datas = line.split("_");
                    if (!"-1".equals(datas[6])) {
                        // 点击的行为
                        return Arrays.asList(
                                new CategoryCount(datas[6], 1, 0, 0)
                        ).iterator();
                    } else if (!"null".equals(datas[8])) {
                        // 下单行为
                        final String[] ids = datas[8].split(",");
                        List<CategoryCount> ccs = new ArrayList<>();
                        for (String id : ids) {
                            ccs.add(new CategoryCount(id, 0, 1, 0));
                        }
                        return ccs.iterator();
                    } else {
                        // 支付行为
                        final String[] ids = datas[10].split(",");
                        List<CategoryCount> ccs = new ArrayList<>();
                        for (String id : ids) {
                            ccs.add(new CategoryCount(id, 0, 0, 1));
                        }
                        return ccs.iterator();
                    }
                }
        );
        //flatMapRDD.take(3).forEach(System.out::println);
        // TODO 将数据转换结构，用于分组统计
        // (k, v)
        final JavaPairRDD<String, CategoryCount> mapPairRDD = flatMapRDD.mapToPair(
                obj -> {
                    return new Tuple2<>(obj.getCid(), obj);
                }
        );

        // TODO 将数据进行分组
        // groupByKey + mapValues
        // reduceByKey
        // reduceByKey方法：将相同key的数据分在一个组中，对value进行两两聚合
        //          1, 点击    0       0
        //          1, 0      下单     0
        //          --------------------
        //          1, 点击    下单    0
        //          1, 0       0     支付
        //          ---------------------
        //          1  点击    下单    支付
        final JavaPairRDD<String, CategoryCount> reduceRDD = mapPairRDD.reduceByKey(
                (obj1, obj2) -> {
                    obj1.setClickCount(obj1.getClickCount() + obj2.getClickCount());
                    obj1.setOrderCount(obj1.getOrderCount() + obj2.getOrderCount());
                    obj1.setPayCount(obj1.getPayCount() + obj2.getPayCount());
                    return obj1;
                }
        );
        //reduceRDD.take(3).forEach(System.out::println);
        // TODO 将统计的结果进行转换，只保留value
        final JavaRDD<CategoryCount> valueMapRDD = reduceRDD.map(
                kv -> kv._2
        );
        // TODO 对统计的结果进行排序
        final JavaRDD<CategoryCount> sortedRDD = valueMapRDD.sortBy(obj -> obj, false, 2);

        // TODO 对排序结果取前10名
        final List<CategoryCount> top10 = sortedRDD.take(10);

        top10.forEach(System.out::println);

        sc.stop();
    }
}
// TODO 自定义统计类：品类点击数量
class CategoryCount implements Serializable, Comparable<CategoryCount> {
    private String cid;
    private int clickCount;
    private int orderCount;
    private int payCount;

    @Override
    public int compareTo(CategoryCount other) {
        if ( this.clickCount > other.clickCount ) {
            return 1;
        } else if ( this.clickCount < other.clickCount ) {
            return -1;
        } else {
            if ( this.orderCount > other.orderCount ) {
                return 1;
            } else if ( this.orderCount < other.orderCount ) {
                return -1;
            } else {
                return this.payCount - other.payCount;
            }
        }
    }

    public CategoryCount(String cid, int clickCount, int orderCount, int payCount) {
        this.cid = cid;
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    public CategoryCount() {
    }

    public String getCid() {
        return cid;
    }

    public void setCid(String cid) {
        this.cid = cid;
    }

    public int getClickCount() {
        return clickCount;
    }

    public void setClickCount(int clickCount) {
        this.clickCount = clickCount;
    }

    public int getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(int orderCount) {
        this.orderCount = orderCount;
    }

    public int getPayCount() {
        return payCount;
    }

    public void setPayCount(int payCount) {
        this.payCount = payCount;
    }

    @Override
    public String toString() {
        return "CategoryCount{" +
                "cid=" + cid +
                ", clickCount=" + clickCount +
                ", orderCount=" + orderCount +
                ", payCount=" + payCount +
                '}';
    }
}
