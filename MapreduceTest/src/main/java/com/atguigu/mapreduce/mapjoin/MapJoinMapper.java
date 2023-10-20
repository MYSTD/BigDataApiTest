package com.atguigu.mapreduce.mapjoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private HashMap<String, String> pdMap = new HashMap<>();
    private Text outk = new Text();

    // 用于将缓存的文件pd.txt 通过输入流读取到内存中的pdMap中
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取缓存文件的路径
        URI[] cacheFiles = context.getCacheFiles();
        URI cacheFile = cacheFiles[0];
        // 获取文件系统对象
        FileSystem fs = FileSystem.get(context.getConfiguration());
        // 获取输入流
        FSDataInputStream inputStream = fs.open(new Path(cacheFile));
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(inputStream, "utf-8"));
        String line;
        while (StringUtils.isNotEmpty(line = bufferedReader.readLine())){
            // 将当前行数据读取到 pdMap  01	小米
            // 切割
            String[] pdData = line.split("\t");
            pdMap.put(pdData[0], pdData[1]);
        }

        // 关闭资源
        IOUtils.closeStream(fs);
        IOUtils.closeStream(bufferedReader);
    }

    /**
     * mao端的核心业务逻辑（按行读取order文件，然后获取pid,根据pid到内存中的容器中获取关联数据）
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取当前行数据
        String lineData = value.toString();
        // 切割  1001	01	1
        String[] datas = lineData.split("\t");
        // 获取pid
        String pid = datas[1];
        // 根据pid到pdMap获取pname
        String pname = pdMap.get(pid);
        // 封装写出的结果
        String result = datas[0] + "\t" + pname + "\t" + datas[2];
        outk.set(result);
        context.write(outk, NullWritable.get());
    }
}
