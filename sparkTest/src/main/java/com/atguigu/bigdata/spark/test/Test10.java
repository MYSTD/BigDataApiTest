package com.atguigu.bigdata.spark.test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Test10 {
    public static void main(String[] args) {

        // JDK1.8 => Scala => 马丁
        final List<Integer> nums = Arrays.asList(
                1, 2, 3, 4
        );

//        int sum = 0;
//        for (Integer num : nums) {
//            sum += num;
//        }
//        System.out.println(
//                nums.stream().reduce(
//                    (num1, num2) -> num1 + num2
//                ).get()
//        );
        System.out.println(nums.stream().reduce(Integer::sum).get());

//        System.out.println(sum);

    }
}
