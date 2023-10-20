package com.atguigu.mapreduce.compress;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 1. 自定义的Reducer需要继承Hadoop提供的Reducer
 * 2. 规定Reducer端的输入和输出的数据类型
 * -- 输入的key和value
 * KEYIN, map端输出的key Text
 * VALUEIN, map端输出的value  IntWritable
 * -- 输出的key和value
 * KEYOUT, 就是一个单词  Text
 * VALUEOUT 当前一个单词出现的总次数  IntWritable
 *
 * 3. 重写父类的reduce() 方法，在方法中实现Reduce阶段的业务逻辑
 *
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable outv = new IntWritable();

    /**
     * reduce端的核心业务逻辑（针对当前相同key的一组values进行累加汇总）
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        // 遍历当前相同key对应的一组values
        for (IntWritable value : values) {
            sum+=value.get();
        }
        outv.set(sum);
        // 输出
        context.write(key, outv);
    }
}
