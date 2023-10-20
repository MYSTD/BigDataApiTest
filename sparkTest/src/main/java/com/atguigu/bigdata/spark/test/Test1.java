package com.atguigu.bigdata.spark.test;

import java.util.ArrayList;
import java.util.List;

public class Test1 {
    public static void main(String[] args) {
//        List nums = new ArrayList();
//        nums.add(1);
//        nums.add("abc");
        // 泛型和多态无关
        // 类型和多态相关
        ArrayList<String> nums1 = new ArrayList<String>();
        test(nums1);

//        nums1.clone();
//
//        ArrayList<String> nums2 = new ArrayList<String>();
//        nums2.clone();
////        nums.add(1);
////        String s = "";
//        Object user = new User();
//        User user = new User();

    }
    private static void test( List<String> nums1 ) {
        System.out.println(nums1);
    }
}