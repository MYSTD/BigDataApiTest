package com.atguigu.bigdata.spark.sql;

import org.apache.spark.sql.*;

public class SparkSQL09_Source_CSV {
    public static void main(String[] args) {

        final SparkSession session =
            SparkSession.builder().master("local[*]").appName("SparkSQL").getOrCreate();

        // TODO 读文件
        final Dataset<Row> csvData = session.read()
                .option("header", "true")//默认为false 不读取列名
                .option("sep", ",") // 默认为, 列的分割
                .csv("data/user.csv");
        //.show();

        // TODO 写文件
        // 写文件数据的时候，担心数据会被覆盖掉，为了避免这种情况，所以如果路径已经存在，那么会发生错误。
        // 如果明确知道数据不会被覆盖，可以更改保存模式
        csvData.write()
                .mode("append")
                .csv("csv");


        //csv.createOrReplaceTempView("user");

        //session.sql("select avg(_c1) from user").show();

        session.stop();

    }
}
