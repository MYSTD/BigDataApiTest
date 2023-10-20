package com.atguigu.mapreduce.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, OrderPd> {

    private Text outk = new Text();
    private OrderPd outv = new OrderPd();

    private String fileName;

    // 在MapTask任务执行之前会执行一次 本案例中用其获取当前切片对象
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();
    }

    /**
     * Map端的核心业务逻辑（针对每一行数据切割，切割后给输出的key和value进行数据整合）
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
        // 切割
        String[] datas = lineData.split("\t");
        // 区分数据来源
        if(fileName.endsWith("order.txt")){
            // 当前数据来源order文件   1001	01	1
            // 封装输出的key和value
            outk.set(datas[1]);
            outv.setOrderId(Integer.parseInt(datas[0]));
            outv.setPid(Integer.parseInt(datas[1]));
            outv.setAmount(Integer.parseInt(datas[2]));
            outv.setPname("");
            outv.setTitle(fileName);
        }else {
            // 当前数据来源pd文件   01	小米
            // 封装输出的key和value
            outk.set(datas[0]);
            outv.setOrderId(0);
            outv.setPid(Integer.parseInt(datas[0]));
            outv.setAmount(0);
            outv.setPname(datas[1]);
            outv.setTitle(fileName);
        }

        // 输出结果
        context.write(outk, outv);


    }
}
