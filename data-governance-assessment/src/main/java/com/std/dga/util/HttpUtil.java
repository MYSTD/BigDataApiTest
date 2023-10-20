package com.std.dga.util;

import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class HttpUtil {

    private static OkHttpClient client = getInstance();

    private static OkHttpClient getInstance() {
        return new OkHttpClient();
    }


    public static String get(String url) {
        // 同构构造器获取request对象
        Request.Builder builder = new Request.Builder();
        Request request = builder.get().url(url).build();
        Call call = client.newCall(request);
        try {
            // 执行访问请求
            Response response = call.execute();
            return response.body().string();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        String url = "http://hadoop102:18080/api/v1/applications/application_1685070947994_0004";

        String jsonResult = get(url);

        System.out.println(jsonResult);
    }

}