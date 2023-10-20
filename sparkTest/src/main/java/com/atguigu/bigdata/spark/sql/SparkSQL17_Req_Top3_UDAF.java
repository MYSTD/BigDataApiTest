package com.atguigu.bigdata.spark.sql;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class SparkSQL17_Req_Top3_UDAF {
    public static void main(String[] args) {

        // TODO 在代码的最上面添加HADOOP权限用户：root
        System.setProperty("HADOOP_USER_NAME","root");

        final SparkSession session =
            SparkSession.builder()
                .enableHiveSupport()// TODO 添加hive支持
                .master("local[*]")
                .appName("SparkSQL")
                .getOrCreate();


        session.sql("use spark");

        session.udf().register("cityRemark", functions.udaf(
                new MyCityRemarkUDAF(), Encoders.STRING()
        ));

/*
select
	area,
	product_name,
	clickCount,
	remark
from (
	select
		area,
		product_name,
		clickCount,
		remark,
		rank() over ( partition by area order by clickCount desc ) rk
	from (
		select
			area,
			product_name,
			count(*) clickCount,
			cityRemark(city_name) remark
		from (
			select
				click_product_id,
				city_id
			from user_visit_action
			where click_product_id != -1
		) act
		join (
			select
				city_id,
				city_name,
				area
			from city_info
		) city on act.city_id = city.city_id
		join (
		   select
			  product_id,
			  product_name
		   from product_info
		) pi on act.click_product_id = pi.product_id
		group by area, product_id, product_name
	) t
) t1 where rk <= 3

 */

        session.sql("select\n" +
                "\tarea,\n" +
                "\tproduct_name,\n" +
                "\tclickCount,\n" +
                "\tremark\n" +
                "from (\n" +
                "\tselect\n" +
                "\t\tarea,\n" +
                "\t\tproduct_name,\n" +
                "\t\tclickCount,\n" +
                "\t\tremark,\n" +
                "\t\trank() over ( partition by area order by clickCount desc ) rk\n" +
                "\tfrom (\n" +
                "\t\tselect\n" +
                "\t\t\tarea,\n" +
                "\t\t\tproduct_name,\n" +
                "\t\t\tcount(*) clickCount,\n" +
                "\t\t\tcityRemark(city_name) remark\n" +
                "\t\tfrom (\n" +
                "\t\t\tselect\n" +
                "\t\t\t\tclick_product_id,\n" +
                "\t\t\t\tcity_id\n" +
                "\t\t\tfrom user_visit_action\n" +
                "\t\t\twhere click_product_id != -1\n" +
                "\t\t) act\n" +
                "\t\tjoin (\n" +
                "\t\t\tselect\n" +
                "\t\t\t\tcity_id,\n" +
                "\t\t\t\tcity_name,\n" +
                "\t\t\t\tarea\n" +
                "\t\t\tfrom city_info\n" +
                "\t\t) city on act.city_id = city.city_id\n" +
                "\t\tjoin (\n" +
                "\t\t   select\n" +
                "\t\t\t  product_id,\n" +
                "\t\t\t  product_name\n" +
                "\t\t   from product_info\n" +
                "\t\t) pi on act.click_product_id = pi.product_id\n" +
                "\t\tgroup by area, product_id, product_name\n" +
                "\t) t\n" +
                ") t1 where rk <= 3").show(20, false);

        session.stop();

    }
}
