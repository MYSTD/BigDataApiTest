package com.atguigu.bigdata.spark.sql;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;
import org.jetbrains.annotations.NotNull;
import scala.Serializable;

import java.util.*;

/**
 * TODO 自定义城市备注UDAF函数类
 *    1. 继承org.apache.spark.sql.expressions.Aggregator类
 *    2. 定义泛型
 *       IN : 输入数据的类型
 *       BUFF : 缓冲区的数据类型
 *       OUT : 输出数据的类型
 *    3. 重写方法 （4 + 2）
 *      2个方法和编码相关
 *      4个方法和计算相关
 */
public class MyCityRemarkUDAF extends Aggregator<String, CityRemarkBuffer, String> {
    @Override
    // TODO 缓冲区的初始化操作
    public CityRemarkBuffer zero() {
        return new CityRemarkBuffer( 0, new HashMap<String, Integer>() );
    }

    @Override
    // TODO 将输入的城市和当前的缓冲区进行聚合处理
    public CityRemarkBuffer reduce(CityRemarkBuffer buffer, String city) {
        buffer.setTotalcnt( buffer.getTotalcnt() + 1 );

        final Map<String, Integer> cityMap = buffer.getCityMap();
//        final Integer cityCount = cityMap.get(city);
//        if ( cityCount == null ) {
//            cityMap.put(city, 1);
//        } else {
//            cityMap.put(city, cityCount + 1);
//        }
        cityMap.merge(city, 1, Integer::sum);
        buffer.setCityMap(cityMap);

        return buffer;
    }

    @Override
    // TODO 因为SparkSQL执行的时候是分布式的，所以存在多个缓冲区，需要将多个缓冲区的数据进行合并
    public CityRemarkBuffer merge(CityRemarkBuffer b1, CityRemarkBuffer b2) {
        // TODO 总数量相加
        b1.setTotalcnt( b1.getTotalcnt() + b2.getTotalcnt() );
        // TODO 每个城市的点击数量相加
        //      Map的合并
        //      { a = 1, b = 2 }, { b = 3, c = 4 }
        //      => { a = 1, b = 2 + 3 = 5, c= 4 }
        final Map<String, Integer> map1 = b1.getCityMap();
        final Map<String, Integer> map2 = b2.getCityMap();
        final Iterator<String> cityIter2 = map2.keySet().iterator();
        while ( cityIter2.hasNext() ) {
            final String city2 = cityIter2.next();

            final Integer cityCnt1 = map1.get(city2);
            final Integer cityCnt2 = map2.get(city2);
            if ( cityCnt1 == null ) {
                map1.put(city2, cityCnt2);
            } else {
                map1.put(city2, cityCnt1 + cityCnt2);
            }
        }
        b1.setCityMap(map1);

        return b1;
    }

    @Override
    // TODO 表示计算最终结果
    public String finish(CityRemarkBuffer buffer) {
        StringBuilder ss = new StringBuilder();

        final int totalcnt = buffer.getTotalcnt();
        final Map<String, Integer> cityMap = buffer.getCityMap();
        // 将无序的集合数据进行有序的排列
        // 将排序后的数据取前两名，进行计算，拼接字符串
        // 如果数据超过2个，还需要添加额外的数据结果
        // Spark中很多的功能其实是参考了Scala语言的方法
        // Scala语言基于Java语言开发,推动了Java的发展，JDK1.8就来自于Scala语言
        List<CityCount> list = new ArrayList<CityCount>();
        cityMap.forEach(
            (k, v) -> list.add( new CityCount( k, v ) )
        );
        Collections.sort(list);

        int rest = 100;
        int index = 0;
        for (CityCount cityCount : list) {
            // 100 * 100 / 1000 => 10 + %
            int r = cityCount.getCount() * 100 / totalcnt;
            ss.append( cityCount.getCity() + " " + r + "%, " );
            rest -= r;
            index += 1;
            if ( index == 2 ) {
                break;
            }
        }

        if ( list.size() > 2 ) {
//            ss.append( "其他 " + rest + "%" );
            ss.append("其他 ").append(rest).append("%");
        }

        return ss.toString();
    }

    @Override
    public Encoder<CityRemarkBuffer> bufferEncoder() {
        return Encoders.bean(CityRemarkBuffer.class);
    }

    @Override
    public Encoder<String> outputEncoder() {
        return Encoders.STRING();
    }
}
class CityCount implements Serializable, Comparable<CityCount> {
    private String city;
    private Integer count;

    public CityCount(String city, Integer count) {
        this.city = city;
        this.count = count;
    }

    public CityCount() {
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public int compareTo(@NotNull CityCount other) {
        return other.count - this.count;
    }
}