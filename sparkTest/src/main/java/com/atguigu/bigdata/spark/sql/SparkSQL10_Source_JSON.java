package com.atguigu.bigdata.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkSQL10_Source_JSON {
    public static void main(String[] args) {

        final SparkSession session =
            SparkSession.builder().master("local[*]").appName("SparkSQL").getOrCreate();

        // TODO 读文件
        // JSON文件中的数据要求整个文件的数据格式满足JSON格式
        // SparkSQL是基于SparkCore，SparkCore是基于MR，所以读取文件其实用的就是Hadoop
        // Hadoop读取文件是按行读取，所以要求读取的数据，一行应该符合JSON格式
//        final Dataset<Row> json = session.read().json("data/word.txt");
//
//        json.write().json("json");

        final Dataset<Row> csvData = session.read()
                .option("header", "true")//默认为false 不读取列名
                .option("sep", ",") // 默认为, 列的分割
                .csv("data/user.csv");

        csvData.write().mode(SaveMode.Append).json("json");

        //json.show();

        session.stop();

    }
}
