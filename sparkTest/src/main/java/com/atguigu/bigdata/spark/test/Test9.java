package com.atguigu.bigdata.spark.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Test9 {
    public static void main(String[] args) {

        final List<String> strings = Arrays.asList(
                "hive", "spark", "hello", "hadoop"
        );
        List<String> newList = new ArrayList<>();
        for (String string : strings) {
            if ( string.substring(0,1).equals("s") ) {
                newList.add(string);
            }
        }
        System.out.println(newList);


//        System.out.println(strings.remove("hive"));
    }
}
