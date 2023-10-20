package com.atguigu.bigdata.spark.test;

import java.util.*;

public class Test6 {
    public static void main(String[] args) {

        List<String> ss = Arrays.asList("zhangsan", "lisi", "wangwu");

        for ( String name : ss ) {
            test(name);
        }
    }

    public static void test( String s ) {
        System.out.println(s.substring(0,1).toUpperCase() + s.substring(1));
    }
}
