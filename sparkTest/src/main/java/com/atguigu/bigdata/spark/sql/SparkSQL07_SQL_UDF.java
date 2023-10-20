package com.atguigu.bigdata.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class SparkSQL07_SQL_UDF {
    public static void main(String[] args) {

        final SparkSession session =
            SparkSession.builder().master("local[*]").appName("SparkSQL").getOrCreate();

        final Dataset<Row> jsonRow = session.read().json("data/user.json");

        jsonRow.createOrReplaceTempView("user");

        // SQL : 面向数据库(mysql, oracle)的操作语言
        //       where a = 1 and b = 2
        // Name:zhangsan
        // Name:lisi
        // Name:wangwu
        // SQL -> Hive     -> MR  -> run
        // SQL -> SparkSQL -> RDD -> run

        // TODO 如果SQL文在使用时，某些功能不能实现或不容易实现的时候，可以采用代码的方式实现
        //      这里的代码实现不是全部用代码实现，只是将代码进行封装，封装后在SQL中使用

        //      用户自己随意开发一个函数功能，SparkSQL无法识别，SQL文中就无法使用
        //      自己定义的函数需要向Spark进行注册：函数名称 + 函数逻辑
        session.udf().register("prefixName", new UDF1<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return "Name :" + s;
            }
        }, DataTypes.StringType);


        session.sql("select prefixName(name) from user").show();

        session.stop();

    }
}
