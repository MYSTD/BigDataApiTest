package com.atguigu.mapreduce.compress;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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
        // 在Map阶段输出位置添加压缩
//        conf.set("mapreduce.map.output.compress","true");
        conf.setBoolean("mapreduce.map.output.compress",true);
        conf.setBoolean("mapreduce.output.fileoutputformat.compress",true);
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

        // 指定输入输出路径
        FileInputFormat.addInputPath(job, new Path("D:\\input\\jianai"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\output\\jianai_out3"));
        // 提交
        job.waitForCompletion(false);
    }
}
