package com.atguigu.interceptor;

import com.alibaba.fastjson.JSONObject;

public class Test01 {

    public static void main(String[] args) {

        JSONObject jsonObject = JSONObject.parseObject("{\"name\":\"zhangsan\"}");
        System.out.println(jsonObject.getString("name"));

    }
}
