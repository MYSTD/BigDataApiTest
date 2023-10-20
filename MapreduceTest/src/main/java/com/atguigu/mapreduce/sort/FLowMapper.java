package com.atguigu.mapreduce.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FLowMapper extends Mapper<LongWritable, Text, FlowBean,Text > {

    private Text outv = new Text();
    private FlowBean outk = new FlowBean();

    /**
     * Map端的核心业务逻辑（针对一行数据，获取手机号和上下行流量以及计算总流量封装FlowBean,最后输出）
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
        // 切割  6 	84188413	192.168.100.3	www.atguigu.com	4116	1432	200
        String[] datas = lineData.split("\t");
        // 封装输出的key(手机号)
        outv.set(datas[1]);
        // 封装输出的value(FlowBean)
        outk.setUpFlow(Integer.parseInt(datas[datas.length - 3]));
        outk.setDownFlow(Integer.parseInt(datas[datas.length - 2]));
        outk.setSumFlow();

        // 输出
        context.write(outk, outv);
    }
}
