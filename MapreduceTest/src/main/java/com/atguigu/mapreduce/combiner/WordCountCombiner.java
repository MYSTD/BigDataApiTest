package com.atguigu.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 自定义Combiner流程需要继承Hadoop提供的Reducer
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

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
