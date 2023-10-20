package com.atguigu.bigdata.spark.sql;

import org.apache.spark.sql.SparkSession;

public class SparkSQL14_Mock {
    public static void main(String[] args) {

        // TODO 在代码的最上面添加HADOOP权限用户：root
        System.setProperty("HADOOP_USER_NAME","root");

        final SparkSession session =
            SparkSession.builder()
                .enableHiveSupport()// TODO 添加hive支持
                .master("local[*]")
                .appName("SparkSQL")
                .getOrCreate();


        session.sql("use sparksql230529");


        session.sql("CREATE TABLE `user_visit_action`(\n" +
                "  `date` string,\n" +
                "  `user_id` bigint,\n" +
                "  `session_id` string,\n" +
                "  `page_id` bigint,\n" +
                "  `action_time` string,\n" +
                "  `search_keyword` string,\n" +
                "  `click_category_id` bigint,\n" +
                "  `click_product_id` bigint, --点击商品id，没有商品用-1表示。\n" +
                "  `order_category_ids` string,\n" +
                "  `order_product_ids` string,\n" +
                "  `pay_category_ids` string,\n" +
                "  `pay_product_ids` string,\n" +
                "  `city_id` bigint --城市id\n" +
                ")\n" +
                "row format delimited fields terminated by '\\t';");

        session.sql("CREATE TABLE `city_info`(\n" +
                "  `city_id` bigint, --城市id\n" +
                "  `city_name` string, --城市名称\n" +
                "  `area` string --区域名称\n" +
                ")\n" +
                "row format delimited fields terminated by '\\t';");

        session.sql("CREATE TABLE `product_info`(\n" +
                "  `product_id` bigint, -- 商品id\n" +
                "  `product_name` string, --商品名称\n" +
                "  `extend_info` string\n" +
                ")\n" +
                "row format delimited fields terminated by '\\t';");

        session.sql("load data local inpath 'data/user_visit_action.txt' into table user_visit_action;");
        session.sql("load data local inpath 'data/product_info.txt' into table product_info;");
        session.sql("load data local inpath 'data/city_info.txt' into table city_info;");

        session.sql("select * from city_info limit 10").show();

        session.stop();

    }
}
