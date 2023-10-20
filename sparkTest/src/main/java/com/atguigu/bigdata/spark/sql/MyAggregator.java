package com.atguigu.bigdata.spark.sql;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

// TODO 自定义UDAF函数类
//      1. 继承org.apache.spark.sql.expressions.Aggregator类
//      2. 定义泛型
//         2.1 IN : 表示函数的输入参数
//         2.2 BUFF : 表示计算过程中，缓冲区的数据类型
//         2.3 OUT : 表示函数的输出结果
//      3. 重写方法 : 6个方法(4 + 2)
//         2个方法和序列化相关：固定写法 Encoders.bean, Encoders.Long
//         4个方法和计算过程相关: zero, reduce, merge, finish
public class MyAggregator extends Aggregator<Long, AvgAgeBuffer, Long> {
    @Override
    // TODO zero, z表示零值，初始值,类似于java中init
    public AvgAgeBuffer zero() {
        return new AvgAgeBuffer(0L, 0L);
    }

    @Override
    // TODO reduce : 聚合, 将输入的参数值和当前缓冲区的数据进行聚合除了
    public AvgAgeBuffer reduce(AvgAgeBuffer buffer, Long inputAge) {
        buffer.setTotal( buffer.getTotal() + inputAge );
        buffer.setCount( buffer.getCount() + 1 );
        return buffer;
    }

    @Override
    // TODO merge : 合并, 将多个缓冲区进行合并处理
    public AvgAgeBuffer merge(AvgAgeBuffer b1, AvgAgeBuffer b2) {
        b1.setTotal( b1.getTotal() + b2.getTotal() );
        b1.setCount( b1.getCount() + b2.getCount() );
        return b1;
    }

    @Override
    // TODO finish : 完成，表示计算
    public Long finish(AvgAgeBuffer buffer) {
        return buffer.getTotal() / buffer.getCount();
    }

    @Override
    public Encoder<AvgAgeBuffer> bufferEncoder() {
        return Encoders.bean(AvgAgeBuffer.class);
    }

    @Override
    public Encoder<Long> outputEncoder() {
        return Encoders.LONG();
    }
}
