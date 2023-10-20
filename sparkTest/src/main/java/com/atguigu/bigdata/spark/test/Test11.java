package com.atguigu.bigdata.spark.test;

import java.util.Arrays;
import java.util.List;

public class Test11 {
    public static void main(String[] args) {

        final List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5, 6);

        for (Integer num : nums) {
            test2(num);
        }
        System.out.println("********************");
        test1(nums);

    }
    private static void test1(List<Integer> nums) {
        for (Integer num : nums) {
            System.out.println(num);
        }
    }

    private static void test2(Integer num) {
        System.out.println(num);
    }

}
