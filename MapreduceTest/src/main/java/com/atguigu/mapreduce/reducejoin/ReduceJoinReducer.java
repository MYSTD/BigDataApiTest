package com.atguigu.mapreduce.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class ReduceJoinReducer extends Reducer<Text, OrderPd, OrderPd, NullWritable> {

    private ArrayList<OrderPd> orderPds = new ArrayList<>();
    private OrderPd op = new OrderPd();
    /**
     * reduce端核心业务（将当前相同pid的一组数据按照数据来源进行分离,获取pd数据中的pname给order数据中的pname赋值）
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<OrderPd> values, Context context) throws IOException, InterruptedException {
        // 将当前相同key的一组数据进行遍历，根据数据来源分离
        for (OrderPd value : values) {
            if(value.getTitle().endsWith("order.txt")){
                // order文件
                try {
                    OrderPd thisOrderPd = new OrderPd();
                    BeanUtils.copyProperties(thisOrderPd, value);
                    orderPds.add(thisOrderPd);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            }else {
                // pd文件
                try {
                    BeanUtils.copyProperties(op, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        // 将pd的pname获取给order的pname进行赋值
        for (OrderPd orderPd : orderPds) {
            orderPd.setPname(op.getPname());
            // 将结果写出
            context.write(orderPd, NullWritable.get());
        }

        // 清空集合
        orderPds.clear();
    }
}
