package com.atguigu.bigdata.spark.test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class Test {
    public static void main(String[] args) {

        // function : 函数（功能）
        List<Integer> nums = new ArrayList<>();
        nums.add(1);
        nums.add(3);
        nums.add(5);

        int total = 0;

//        for ( Integer num : nums ) {
//            total += num;
//        }
//        System.out.println(total);
        // 函数类
        //nums.forEach( new MyConsumer() );

//        nums.forEach(
//            (num) -> {
//                System.out.println(num);
//            }
//        );

        // 如果参数列表中就一个参数，那么小括号可以省略
        // 如果代码逻辑只有一行，那么大括号可以省略，分号也省
        // 如果参数在逻辑代码中，只使用了了一次，也可以省略,采用特殊语法（::）
        nums.forEach(num -> System.out.println(num));
        nums.forEach(
                System.out::println
        );

//        nums.stream().reduce(
//                (num1, num2) -> {
//                    return Integer.sum(num1, num2);
//                }
//        );
//        nums.stream().reduce(Integer::sum);

    }
}
//class MyConsumer implements Consumer<Integer> {
//    @Override
//    public void accept(Integer num) {
//        System.out.println(num);
//    }
//}