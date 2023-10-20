package com.atguigu.bigdata.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkSQL11_Source_Parquet {
    public static void main(String[] args) {

        final SparkSession session =
            SparkSession.builder().master("local[*]").appName("SparkSQL").getOrCreate();

        // TODO 读文件
        //final Dataset<Row> json = session.read().json("data/user.json");
        final Dataset<Row> parquet = session.read().parquet("data/user.parquet");

        // TODO 如果直接使用save方法。那么表示默认采用parquet格式（列式存储）进行保存
        //      如果想要保存成其他格式，可以进行格式化处理
//        json.write()
//                //.format("json")
//            .save("parquet");

        parquet.show();

        session.stop();

    }
}
