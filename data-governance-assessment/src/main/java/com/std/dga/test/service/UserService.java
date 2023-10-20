package com.std.dga.test.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.std.dga.test.bean.User;

import java.util.List;

/**
 * ClassName:UserService
 * Description:
 *
 * @date:2023/10/7 16:36
 * @author:STD
 */

public interface UserService extends IService<User> {
    User getUser(String name,Integer age);

    List<User> selectUser(String name, Integer age);
}
