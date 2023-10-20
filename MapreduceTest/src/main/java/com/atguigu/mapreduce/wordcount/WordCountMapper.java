package com.atguigu.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 1. 自定义的Mapper需要继承Hadoop提供的Mapper
 * 2. 规定Mapper端的输入和输出的数据类型
 * -- 输入的key和value
 * KEYIN, 当前行数据在输入文件中的偏移量  LongWritable
 * VALUEIN, 当前一行文本数据  Text
 * -- 输出的key和value
 * KEYOUT, 就是一个单词  Text
 * VALUEOUT 给当前单词打标记 1  IntWritable
 *
 * 3. 重写父类的map() 方法，在方法中实现Map阶段的业务逻辑
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outk = new Text();
    private IntWritable outv = new IntWritable(1);

    /**
     * map阶段的核心业务逻辑（针对当前行数据，进行切割，将每个单词打标记1 然后输出）
     * @param key
     * @param value
     * @param context 上下文对象，起到了承上启下的作用
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取当前行数据
        String lineData = value.toString();
        // 切割
        String[] datas = lineData.split(" ");
        // 遍历datas 给每一个元素进行打标记1 然后输出
        for (String data : datas) {
            outk.set(data);
            context.write(outk, outv);
        }
    }
}
