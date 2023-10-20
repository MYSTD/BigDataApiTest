package com.std.dga.test.service.impl;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.std.dga.test.bean.User;
import com.std.dga.test.service.UserService;
import com.std.dga.test.mapper.UserMapper;
import com.std.dga.util.SqlUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * ClassName:UserServiceImpl
 * Description:
 *
 * @date:2023/10/7 16:36
 * @author:STD
 */
@Service
@DS("dga")
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements UserService {

    @Autowired
    UserMapper userMapper;
    @Override
    public User getUser(String name,Integer age) {
        User user = new User(1001L,name,age);
        return user;
    }

    @Override
    public List<User> selectUser(String name, Integer age) {

//        StringBuilder sql = new StringBuilder("select * from user where 1=1 ");
//        if(name != null && !name.isEmpty())
//            sql.append("and name='").append(SqlUtil.filterUnsafeSql(name)).append("'");
//        if(age!=null)
//            sql.append("and age=").append(age);
//        return userMapper.selectUserList(sql.toString());

        List<User> list = userMapper.selectList(new QueryWrapper<User>().eq(
        name != null && !name.isEmpty(), "name",
        SqlUtil.filterUnsafeSql(name)).eq(
        age != null, "age", age));
        return list;

    }


}
