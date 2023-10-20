package com.atguigu.bigdata.spark.test;

public class Test4 {
    public static void main(String[] args) {
        // TODO 调用一个为null的对象的成员属性或成员方法，就会发生空指针异常
        User user = new User();
        // Integer.intValue
        test(user.age);
    }
    private static void test( int age ) {
        System.out.println(age);
    }
}
class User {
    public Integer age;
}
