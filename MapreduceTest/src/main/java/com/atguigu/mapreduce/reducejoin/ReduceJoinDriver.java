package com.atguigu.mapreduce.reducejoin;

import com.atguigu.mapreduce.wordcount.WordCountMapper;
import com.atguigu.mapreduce.wordcount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ReduceJoinDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 声明配置对象
        Configuration conf = new Configuration();
        // 获取Job对象
        Job job = Job.getInstance(conf);
        // 指定当前Mapper和Reducer
        job.setMapperClass(ReduceJoinMapper.class);
        job.setReducerClass(ReduceJoinReducer.class);
        // 指定Map阶段输出的key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderPd.class);
        // 指定当前MR最终输出的key和value的类型
        job.setOutputKeyClass(OrderPd.class);
        job.setOutputValueClass(NullWritable.class);

        // 指定输入输出路径
        FileInputFormat.addInputPath(job, new Path("D:\\input\\reducejoin"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\output\\reducejoin_out101"));
        // 提交
        job.waitForCompletion(true);
    }
}
