package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 自定义OutputFormat需要继承Hadoop提供的OutputFormat类
 */
public class LogOutputFormat extends FileOutputFormat<Text, NullWritable> {


    /**
     * 获取负责写出数据的RecordWriter对象
     * @param job the information about the current task.
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        LogRecordWriter logRecordWriter = new LogRecordWriter(job);
        return logRecordWriter;
    }
}
