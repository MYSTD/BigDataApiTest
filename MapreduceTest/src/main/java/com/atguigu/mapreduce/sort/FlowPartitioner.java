package com.atguigu.mapreduce.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区器对象需要继承Partitioner
 */
public class FlowPartitioner extends Partitioner<FlowBean,Text> {

    /**
     * 核心分区逻辑方法
     * @param text the key to be partioned.
     * @param flowBean the entry value.
     * @param numPartitions the total number of partitions.
     * @return
     * 136 --> 0
     * 137 --> 1
     * 138 --> 2
     * 139 --> 3
     * oyher --> 4
     */
    @Override
    public int getPartition(FlowBean flowBean, Text text,  int numPartitions) {
        int partitions;
        // 获取手机号
        String phoneNum = text.toString();
        if(phoneNum.startsWith("136")){
            partitions = 0;
        } else if (phoneNum.startsWith("137")) {
            partitions = 1;
        } else if (phoneNum.startsWith("138")) {
            partitions = 2;
        } else if (phoneNum.startsWith("139")) {
            partitions = 3;
        }else {
            partitions = 4;
        }
        return partitions;
    }
}
