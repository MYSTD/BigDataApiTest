package com.atguigu.mapreduce.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean,  Text, FlowBean> {
    private FlowBean outv = new FlowBean();
    /**
     * Reduce端的核心业务逻辑（根据当前相同手机号所对应的一组values进行流量汇总）
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int totalUpFlow = 0;
        int totalDownFlow = 0;
//        int totalSumFlow = 0;
        for (FlowBean value : values) {
            totalUpFlow += value.getUpFlow();
            totalDownFlow += value.getDownFlow();
//            totalSumFlow += value.getSumFlow();
        }
        outv.setUpFlow(totalUpFlow);
        outv.setDownFlow(totalDownFlow);
        outv.setSumFlow();

        // 输出
        context.write(key, outv);

    }
}
