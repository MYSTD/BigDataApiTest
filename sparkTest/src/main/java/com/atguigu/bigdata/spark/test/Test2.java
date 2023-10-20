package com.atguigu.bigdata.spark.test;

public class Test2 {
    public static void main(String[] args) {

        String name = "zhangsan"; // zHANGSAN
        String result = headLowercase1(name);
        System.out.println(result); // Zhangsan


    }
    private static String headUppercase(String s) {
        return s.substring(0, 1).toUpperCase() + s.substring(1);
    }
    private static String headLowercase(String s) {
        return s.substring(0, 1).toLowerCase() + s.substring(1);
    }
    private static String headLowercase1(String s) {
        return s.substring(0, 1).toLowerCase() + s.substring(1).toUpperCase();
    }
}
