package com.atguigu.bigdata.spark.test;

import java.util.*;

public class Test8 {
    public static void main(String[] args) {

        final List<String> strings = Arrays.asList("hadoop", "hive", "spark");
        // 【h, [hadoop, hive]】
        // 【s, [spark]】
        Map<String, List<String>> groupMap = new HashMap<String, List<String>>();

        for (String word : strings) {
            String w = word;//word.substring(0, 2);
            List<String> values = groupMap.get(w);
            if ( values == null ) {
                values = new ArrayList<String>();
                values.add(word);
                groupMap.put(w, values);
            } else {
                values.add(word);
            }
        }
        System.out.println(groupMap);

    }
}
