package com.atguigu.hive.fun;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
* 面向插件开发
*   1. 继承hive提供的父级组件（抽象类，接口）
*   2. 重写实现父组件中的方法 实现自定义逻辑
*   3. 打包上传到指定开发环境
*   4. 使用自定义功能
*
*   函数需求：计算一个字符串的长度  select my_len("hello"); --> 5
* */
public class MyHiveFunction extends GenericUDF {
    /**
     * 初始化方法（校验）
     * @param arguments
     *          The ObjectInspector for the arguments
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        // 1. 对参数进行非空判断
        if(arguments != null && arguments.length != 1){
            throw new UDFArgumentLengthException("参数不能为空!!!");
        }

        // 2. 对数据类型进行校验
        ObjectInspector argument = arguments[0];
        if(!argument.getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
            throw new UDFArgumentTypeException(0,"当前参数数据类型不是基本数据类型!!!");
        }

        // 3. 校验当前参数是否为string类型
        PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) arguments[0];
        if(!primitiveObjectInspector.getPrimitiveCategory()
                .equals(PrimitiveObjectInspector.PrimitiveCategory.STRING)){
            throw new UDFArgumentTypeException(0,"当前参数数据类型不是String类型!!!");
        }

        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /**
     * 当前函数的核心逻辑
     * @param arguments
     *          The arguments as DeferedObject, use DeferedObject.get() to get the
     *          actual argument Object. The Objects can be inspected by the
     *          ObjectInspectors passed in the initialize call.
     * @return
     * @throws HiveException
     * TODO: Hive是一种懒加载机制
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        DeferredObject argument = arguments[0];
        Object o = argument.get(); //懒加载
        if(o == null){
            return 0;
        }
        return o.toString().length();
    }

    // 针对当前函数封装一些描述信息
    @Override
    public String getDisplayString(String[] children) {
        return null;
    }
}
