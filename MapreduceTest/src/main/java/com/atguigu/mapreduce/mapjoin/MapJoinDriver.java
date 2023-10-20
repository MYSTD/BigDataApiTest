package com.atguigu.mapreduce.mapjoin;

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
import java.net.URI;
import java.net.URISyntaxException;

public class MapJoinDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        // 声明配置对象
        Configuration conf = new Configuration();
        // 获取Job对象
        Job job = Job.getInstance(conf);
        // 指定当前Mapper
        job.setMapperClass(MapJoinMapper.class);
        // 指定Map阶段输出的key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 指定当前MR最终输出的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 将ReduceTask的数量设置为0
        job.setNumReduceTasks(0);
        // 设置分布式缓存文件
        job.addCacheFile(new URI("file:///D:/input/cachefile/pd.txt"));

        // 指定输入输出路径
        FileInputFormat.addInputPath(job, new Path("D:\\input\\mapjoin"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\output\\mapjoin_out3"));
        // 提交
        job.waitForCompletion(true);
    }
}
