package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private String atguiguPath = "D:\\outlog\\atguigu.log";
    private String otherPath = "D:\\outlog\\other.log";

    private FileSystem fs;
    private FSDataOutputStream atguiguOut;
    private FSDataOutputStream otherOut;

    /**
     * 通过有参构造器将job传入到当前类
     *
     * @param job
     */
    public LogRecordWriter(TaskAttemptContext job) throws IOException {
        // 获取文件系统对象
        fs = FileSystem.get(job.getConfiguration());
        // 获取输出流
        atguiguOut = fs.create(new Path(atguiguPath));
        otherOut = fs.create(new Path(otherPath));
    }

    // 写数据的方法
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        // 获取key
        String data = key.toString();
        if(data.contains("atguigu")){
            atguiguOut.writeBytes(data + "\n");
        }else {
            otherOut.writeBytes(data + "\n");
        }

    }

    // 关闭资源
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(fs);
        IOUtils.closeStream(atguiguOut);
        IOUtils.closeStream(otherOut);
    }
}
