package com.atguigu.bigdata.spark.sql;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;

public class SparkSQL08_SQL_UDAF {
    public static void main(String[] args) {

        final SparkSession session =
            SparkSession.builder().master("local[*]").appName("SparkSQL").getOrCreate();

        final Dataset<Row> jsonRow = session.read().json("data/user.json");

        jsonRow.createOrReplaceTempView("user");

        // TODO UDAF函数一般在使用时，都是采用公共自定义函数类
        session.udf().register("avgAge", functions.udaf(
                new MyAggregator(), Encoders.LONG()
        ));

        // TODO 计算用户的年龄平均值
        session.sql("select avgAge(age) from user").show();

        session.stop();

    }
}
