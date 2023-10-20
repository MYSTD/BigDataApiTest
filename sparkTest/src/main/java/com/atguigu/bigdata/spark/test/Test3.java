package com.atguigu.bigdata.spark.test;

public class Test3 {
    public static void main(String[] args) throws Exception {

        Thread t1 = new Thread();
        Thread t2 = new Thread();

        t1.start();
        t2.start();

        // TODO sleep & wait方法的本质区别就是字体不一样
        t1.sleep(1000); // 静态方法，和对象无关，当前休眠的线程不是t1线程, 休眠的线程为main线程
        t2.wait(1000);  // 成员方法, 和对象相关，当前等待的线程为t2线程

    }
}
