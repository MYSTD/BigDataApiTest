package com.atguigu.bigdata.spark.sql;

import org.apache.spark.sql.SparkSession;

public class SparkSQL16_Req_Top3 {
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

        // TODO 1. 从行为表种获取点击行为数据
        session.sql("select" +
                " click_product_id," +
                " city_id" +
                " from user_visit_action" +
                " where click_product_id != -1").createOrReplaceTempView("act");

        // TODO 2. 从城市表种获取区域信息
        session.sql("select * from city_info").createOrReplaceTempView("city");

        // TODO 3. 从商品信息表中获取商品名称
        session.sql("select product_id, product_name from product_info").createOrReplaceTempView("pi");

        // TODO 4. 将三个查询结果集进行连接操作，补全数据
        session.sql("select area, product_id, product_name " +
                " from act" +
                " join city on act.city_id = city.city_id" +
                " join pi on act.click_product_id = pi.product_id").createOrReplaceTempView("joinData");

        // TODO 5. 将补全的数据进行分组聚合
        session.sql("select area, product_name, count(*) clickCount" +
                " from joinData" +
                " group by area, product_id, product_name").createOrReplaceTempView("groupData");

        // TODO 6. 给数据增加分组排序标记
        session.sql("select " +
                " area, product_name, clickCount," +
                " rank() over ( partition by area order by clickCount desc ) rk" +
                " from groupData").createOrReplaceTempView("rankData");

        // TODO 7. 将增加标记后的数据取前3名
        session.sql("select * from rankData where rk <= 3").show();


//        session.sql("select\n" +
//                "\tarea,\n" +
//                "\tproduct_name,\n" +
//                "\tclickCount,\n" +
//                "\trk\n" +
//                "from (\n" +
//                "\tselect\n" +
//                "\t\tarea,\n" +
//                "\t\tproduct_name,\n" +
//                "\t\tclickCount,\n" +
//                "\t\trank() over ( partition by area order by clickCount desc ) rk\n" +
//                "\tfrom (\n" +
//                "\t\tselect\n" +
//                "\t\t\tarea,\n" +
//                "\t\t\tproduct_name,\n" +
//                "\t\t\tcount(*) clickCount\n" +
//                "\t\tfrom (\n" +
//                "\t\t\tselect\n" +
//                "\t\t\t\tclick_product_id,\n" +
//                "\t\t\t\tcity_id\n" +
//                "\t\t\tfrom user_visit_action\n" +
//                "\t\t\twhere click_product_id != -1\n" +
//                "\t\t) act\n" +
//                "\t\tjoin (\n" +
//                "\t\t\tselect\n" +
//                "\t\t\t\tcity_id,\n" +
//                "\t\t\t\tcity_name,\n" +
//                "\t\t\t\tarea\n" +
//                "\t\t\tfrom city_info\n" +
//                "\t\t) city on act.city_id = city.city_id\n" +
//                "\t\tjoin (\n" +
//                "\t\t   select\n" +
//                "\t\t\t  product_id,\n" +
//                "\t\t\t  product_name\n" +
//                "\t\t   from product_info\n" +
//                "\t\t) pi on act.click_product_id = pi.product_id\n" +
//                "\t\tgroup by area, product_id, product_name\n" +
//                "\t) t\n" +
//                ") t1 where rk <= 3 limit 10").show();


/*
   将数据进行分组的方式，一般存在2种
   1. 将数据分组后要聚合：N -> 1 (group by)
   2. 将数据分组后不聚合，增加标记加以区分：N -> N (开窗)

 */

        session.stop();

    }
}
