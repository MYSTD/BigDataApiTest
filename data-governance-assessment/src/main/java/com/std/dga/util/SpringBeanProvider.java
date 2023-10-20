package com.std.dga.util;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * 负责从Spring的容器中获取组件对象
 */
@Component
public class SpringBeanProvider implements ApplicationContextAware {

    ApplicationContext applicationContext ;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext ;
    }

    /**
     * 通过组件的名字，从容器中获取到对应的组件对象
     */
    public  <T> T getBeanByName(String beanName , Class<T> tClass){
        T bean = applicationContext.getBean(beanName, tClass);
        return bean ;
    }
}
