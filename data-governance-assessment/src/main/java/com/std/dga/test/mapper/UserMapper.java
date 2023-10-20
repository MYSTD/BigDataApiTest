package com.std.dga.test.mapper;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.std.dga.test.bean.User;
import org.apache.ibatis.annotations.*;

import java.util.List;

/**
 * ClassName:UserMapper
 * Description:
 *
 * @date:2023/10/7 20:34
 * @author:STD
 */
@Mapper
@DS("test")
public interface UserMapper extends BaseMapper<User> {
    @Select("select * from user")
    public List<User> getUserList();

    @Select("select * from user  where id=#{id}")
    public User getUserById(Long id );

    @Select("${sql}")
    public List<User> selectUserList(String sql);

    @Insert("insert into user(name,age) values (#{user.name}, #{user.age} )")
    public void insertUser(@Param("customer") User user);

    @Update("update  user  set age= #{user.age}  where id=#{ user.id }")
    public void updateUser (@Param("user") User user );


    @Delete("delete user where id=#{id}")
    public void deleteUserById (Long id  );

}
