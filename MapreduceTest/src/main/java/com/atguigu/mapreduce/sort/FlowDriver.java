package com.atguigu.mapreduce.sort;

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
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        // 指定最终输出的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        // 指定ReduceTask数量
        job.setNumReduceTasks(5);
        job.setPartitionerClass(FlowPartitioner.class);
        // 指定比较器对象
        job.setSortComparatorClass(FlowWritableComparator.class);
        // 指定输入输出路径
        FileInputFormat.addInputPath(job, new Path("D:\\input\\phone_data"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\output\\phone_data_out3"));
        // 提交
        job.waitForCompletion(true);
    }
}
