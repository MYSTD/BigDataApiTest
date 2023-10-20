package com.atguigu.bigdata.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SparkSQL12_Source_MySQL {
    public static void main(String[] args) {

        final SparkSession session =
            SparkSession.builder().master("local[*]").appName("SparkSQL").getOrCreate();

        // TODO 读取MySQL
        //Dataset<Row> json = session.read().json("input/user.json");

        Properties properties = new Properties();
        properties.setProperty("user","root");
        properties.setProperty("password","123123");

//        json.write()
//                // 写出模式针对于表格追加覆盖
//                .mode(SaveMode.Append)
//                .jdbc("jdbc:mysql://hadoop102:3306","gmall.testInfo",properties);

        Dataset<Row> jdbc = session.read().jdbc(
                "jdbc:mysql://linux1:3306/gmall?useSSL=false&useUnicode=true&characterEncoding=UTF-8&allowPublicKeyRetrieval=true",
                "activity_info",
                properties);

        jdbc.show();

        session.stop();

    }
}
