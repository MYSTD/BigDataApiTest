package com.atguigu.mapreduce.combinetextinputformat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MR的驱动类（用于提交job）
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 声明配置对象
        Configuration conf = new Configuration();
        // 通过配置文件修改InputFormat的实现类
//        conf.set("mapreduce.job.inputformat.class","org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat");
        // 获取Job对象
        Job job = Job.getInstance(conf);
        // 指定当前Mapper和Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // 指定Map阶段输出的key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 指定当前MR最终输出的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 在job中指定InputFormat的实现类
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 手动设定切片大小
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304 * 10);

        // 指定输入输出路径
        FileInputFormat.addInputPath(job, new Path("D:\\input\\combine"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\output\\combine_out5"));
        // 提交
        job.waitForCompletion(true);
    }
}
