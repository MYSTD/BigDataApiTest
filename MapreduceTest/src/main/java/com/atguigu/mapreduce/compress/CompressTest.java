package com.atguigu.mapreduce.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.*;

/**
 * 作业：使用Hadoop提供的压缩方式压缩解压缩实现
 *
 * 需求：已知有一份普通文件 ja.txt 要求将其进行压缩
 *      并且将 xxx.xx压缩解压缩成普通文件
 */
public class CompressTest {


    /**
     * 压缩：本质就是通过具备压缩功能的输出流将数据写出到磁盘上
     */
    @Test
    public void testCompress() throws IOException, ClassNotFoundException {
        // 声明待压缩文件的路径
        String inputPath = "D:\\input\\compress\\ja.txt";
        // 声明压缩结果文件的输出路径
        String outputPath = "D:\\input\\compress\\ja";

        // 声明输入流
        FileInputStream inputStream = new FileInputStream(new File(inputPath));
        // 指定要使用的压缩解压缩对象  org.apache.hadoop.io.compress.DefaultCodec
//        DefaultCodec defaultCodec = new DefaultCodec();
        Configuration conf = new Configuration();
        String defaultClass= "org.apache.hadoop.io.compress.DefaultCodec";
        Class<?> codecClass = Class.forName(defaultClass);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        // 声明输出流
        FileOutputStream outputStream = new FileOutputStream(new File(outputPath + codec.getDefaultExtension()));
        CompressionOutputStream codecOutputStream = codec.createOutputStream(outputStream);
        // 数据读写
        IOUtils.copyBytes(inputStream, codecOutputStream, conf);

        // 关流
        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(codecOutputStream);

    }



    /**
     * 解压缩：本质就是通过具备解压缩功能的输入流将数据解压缩到内存中，最后再通过一个常规输出流将数据写出
     */
    @Test
    public void testCompress1() throws IOException, ClassNotFoundException {
        // 声明待压缩文件的路径
        String inputPath = "D:\\input\\compress\\ja.deflate";
        // 声明压缩结果文件的输出路径
        String outputPath = "D:\\input\\compress\\ja.txt";
        Configuration conf = new Configuration();
        // 通过压缩文件的扩展名获取对应编解码器对象
        CompressionCodec codec =
                new CompressionCodecFactory(conf).getCodec(new Path(inputPath));
        // 声明输入流
        FileInputStream inputStream = new FileInputStream(new File(inputPath));
        // 利用 codec 包装输入流
        CompressionInputStream codecInputStream = codec.createInputStream(inputStream);
        // 声明一个输出流
        FileOutputStream outputStream = new FileOutputStream(new File(outputPath));

        // 数据读写
        IOUtils.copyBytes(codecInputStream, outputStream, conf);

        // 关流
        IOUtils.closeStream(codecInputStream);
        IOUtils.closeStream(outputStream);

    }


}
