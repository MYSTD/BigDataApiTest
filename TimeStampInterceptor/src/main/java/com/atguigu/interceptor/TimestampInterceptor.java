package com.atguigu.interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

// // 用户行为数据同步 下游Flume的拦截器
public class TimestampInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    // 处理单个event的逻辑
    // 获取event中的headers和body 然后将body中的ts为key的值封装到headers中
    @Override
    public Event intercept(Event event) {
        // 获取header和body
        Map<String, String> headers = event.getHeaders();
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);
        try {
            // 将log转化成json对象
            JSONObject jsonObject = JSONObject.parseObject(log);
            String timestampStr = jsonObject.getString("ts");
            // 重新指定header的内容
            headers.put("timestamp",timestampStr);
            return event;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // 批量处理event
    @Override
    public List<Event> intercept(List<Event> list) {
        Iterator<Event> iterator = list.iterator();
        while (iterator.hasNext()){
            Event event = intercept(iterator.next());
            if(event == null){
                iterator.remove();
            }
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class MyBuilder implements Builder{

        @Override
        public Interceptor build() {
            return new TimestampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}