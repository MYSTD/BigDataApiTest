package com.atguigu.bigdata.spark.core.rdd.part;

import org.apache.spark.Partitioner;

public class MyPartitioner extends Partitioner {
    @Override
    public int numPartitions() {
        return 2;
    }

    @Override
    public int getPartition(Object key) {
        if ( "nba".equals(key) ) {
            return 0;
        } else {
            return 1;
        }
    }
}
