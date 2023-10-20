package com.atguigu.mapreduce.outputformat;

import com.atguigu.mapreduce.waritable.FLowMapper;
import com.atguigu.mapreduce.waritable.FlowBean;
import com.atguigu.mapreduce.waritable.FlowReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 声明配置对象
        Configuration conf = new Configuration();
        // 获取Job对象
        Job job = Job.getInstance(conf);
        // 指定当前MR的Mapper和Reducer
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);
        // 指定Map端输出的key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 指定最终输出的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 指定OutputFormat的实现
        job.setOutputFormatClass(LogOutputFormat.class);
        // 指定输入输出路径
        FileInputFormat.addInputPath(job, new Path("D:\\input\\log"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\output\\log_out1"));
        // 提交
        job.waitForCompletion(true);
    }
}
