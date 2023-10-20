package com.atguigu.bigdata.spark.sql;

import org.apache.spark.sql.SparkSession;

public class SparkSQL15_Req_Top3 {
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

        // TODO 各区域热门商品Top3
        //     商品点击数量Top3
        //     1. 读取数据的时候，只保留点击数据
        //     2. 转换数据类型：line => (商品A，1)，(商品A，1)，(商品A，1)
        //     3. 将相同的商品分在一个组内，获取点击数量总和
        //     4. 将统计的结果进行排序（降序）
        //     5. 将排序结果获取前3名
        /*
           华北   鞋    2000    1
           华北   衣服  1800    2
           华北   帽子  1500    3
           东北   帽子  2500    1
           东北   衣服  1900    2
           东北   鞋   1550     3
           ---------------------
           ( (华北，鞋)，2000 )
           ( (华北，衣服)，2800 )
           ( (东北，鞋)，2000 )

           1. 将区域和商品作为整体进行分组聚合
           2. 将相同区域的统计结果放置在一起
               ( (华北，鞋)，2000 )
               ( (华北，衣服)，2800 )
           3. 组内进行排序,取前3名
               ( (华北，衣服)，2800 )
               ( (华北，鞋)，2000 )

         */

/*
t_test
--------------------------
prv		city	amount
河北	北京	1000
河北	天津	2000
河北	北京	3000
河南	郑州	4000
-------------------------
需求1：各个省份销售金额总和
select
    prv,
    sum(amount)
from t_test
group by prv
---------------------
prv		totalamount
河北	6000
河南	4000

需求2：各个城市销售金额总和
select
    city,
    sum(amount)
from t_test
group by city
---------------------
city		totalamount
北京		4000
天津		2000
郑州		4000

需求3：各个省份各个城市销售金额总和
select
    prv,
    city,
    sum(amount)
from t_test
group by prv, city
---------------------
prv		city		totalamount
河北	北京		4000
河北	天津		2000
河南	郑州		4000

如果sql文包含了多个字段参与分组，需要考虑字段的关系
1. 如果字段之间存在上下级关系，从属关系，那么统计结果其实就是最低级的统计结果
	上级字段纯粹用于补全数据，不影响统计结果

2. 如果字段之间存在关联关系（id, name）,统计结果会以具有唯一性的字段为准
	其他字段纯粹用于补全数据，不影响统计结果

3. 如果字段之间不存在任何关系，统计结果就以全部字段为准
 */

        /*


-- 如果select语句包含聚合函数，其他的查询字段需要遵循规则
	-- 1. 常量
	-- 2. 字段要在聚合函数内
	-- 3. 字段必须要参与分组（group by）

select
	area,
	product_name,
	clickCount,
	rk
from (
	select
		area,
		product_name,
		clickCount,
		rank() over ( partition by area order by clickCount desc ) rk
	from (
		select
			area,
			product_name,
			count(*) clickCount
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
                "\trk\n" +
                "from (\n" +
                "\tselect\n" +
                "\t\tarea,\n" +
                "\t\tproduct_name,\n" +
                "\t\tclickCount,\n" +
                "\t\trank() over ( partition by area order by clickCount desc ) rk\n" +
                "\tfrom (\n" +
                "\t\tselect\n" +
                "\t\t\tarea,\n" +
                "\t\t\tproduct_name,\n" +
                "\t\t\tcount(*) clickCount\n" +
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
                ") t1 where rk <= 3 limit 10").show();


/*
   将数据进行分组的方式，一般存在2种
   1. 将数据分组后要聚合：N -> 1 (group by)
   2. 将数据分组后不聚合，增加标记加以区分：N -> N (开窗)

 */

        session.stop();

    }
}
