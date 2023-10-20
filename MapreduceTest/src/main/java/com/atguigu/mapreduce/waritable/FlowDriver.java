package com.atguigu.mapreduce.waritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 声明配置对象
        Configuration conf = new Configuration();
        // 获取Job对象
        Job job = Job.getInstance(conf);
        // 指定当前MR的Mapper和Reducer
        job.setMapperClass(FLowMapper.class);
        job.setReducerClass(FlowReducer.class);
        // 指定Map端输出的key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        // 指定最终输出的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        // 指定输入输出路径
        FileInputFormat.addInputPath(job, new Path("D:\\hadoop_testdatas\\writable"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoop_testdatas\\output1"));
        // 提交
        job.waitForCompletion(true);
    }
}
