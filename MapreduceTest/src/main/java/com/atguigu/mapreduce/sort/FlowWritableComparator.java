package com.atguigu.mapreduce.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义FlowBean的比较器对象需要继承Hadoop提供的WritableComparator
 */
public class FlowWritableComparator extends WritableComparator {

    //通过无参构造器指定参与比较的对象
    public FlowWritableComparator() {
        super(FlowBean.class, true);
    }

    // 定义比较规则
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        FlowBean abean = (FlowBean) a;
        FlowBean bbean = (FlowBean) b;
        return abean.getSumFlow().compareTo(bbean.getSumFlow());
    }
}
