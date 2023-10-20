package com.atguigu.bigdata.spark.test;

public class Test5 {
    public static void main(String[] args) {

        Student s = new Student();
        s.test();
    }
}
class Person {
    public int age = 10;
}
class Student extends Person {
    public int age = 20;

    public void test() {
        System.out.println(this.age);
        System.out.println(super.age);
    }
}
