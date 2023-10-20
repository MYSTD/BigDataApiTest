package com.atguigu.bigdata.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SparkSQL13_Source_Hive {
    public static void main(String[] args) {

        final SparkSession session =
            SparkSession.builder()
                .enableHiveSupport()// TODO 添加hive支持
                .master("local[*]")
                .appName("SparkSQL")
                .getOrCreate();

        // TODO 读取Hive
        //      1. 增加依赖关系
        //      2. 增加配置文件 hive-site.xml
        //      3. 代码中启用Hive的支持
        //      4. 使用SQL访问Hive
        session.sql("show tables").show();

        session.stop();

    }
}
