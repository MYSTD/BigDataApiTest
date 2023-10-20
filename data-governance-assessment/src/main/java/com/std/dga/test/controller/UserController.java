package com.std.dga.test.controller;

import com.alibaba.fastjson.JSON;
import com.std.dga.test.bean.User;
import com.std.dga.test.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * ClassName:UserController
 * Description:
 *
 * @date:2023/10/7 16:34
 * @author:STD
 */

@RestController
public class UserController {
    public static String SUCCESS = "成功";

    @Autowired
    UserService userService;

    @GetMapping("/test")
    public String test(){
        return SUCCESS;
    }

    /**
     * kv传参
     */
    @GetMapping("/kv_test")
    public User kv_test(@RequestParam("name") String name, @RequestParam("age") Integer age){

        User user = userService.getUser(name, age);
        System.out.println(user);
        return user;
    }
    @GetMapping("/kv_test_user")
    public User kv_test(User user){

        System.out.println(user);
        return user;
    }

    /**
     * 路径拼接传参
     */
    @PostMapping("/path_test/{name}/{age}")
    public String path_test(@PathVariable("name") String name,@PathVariable("age") Integer age){
        User user = userService.getUser(name, age);
        return JSON.toJSONString(user);
    }

    /**
     * 请求体参数
     */
    @GetMapping("/body_test")
    public User body_test(@RequestBody User user){
        System.out.println(user);
        return user;
    }

    @GetMapping("/user/list")
    public List<User> getUserList(){
        List<User> users = userService.list();
        System.out.println(users);
        return users;
    }

    @PostMapping("/user/add")
    public String addUser(@RequestBody User user){
        boolean save = userService.save(user);
        return save?SUCCESS:"失败";
    }


    @GetMapping("/user/select")
    public List<User> selectUser(@RequestParam(value = "name",required = false) String name,
                                 @RequestParam(value = "age",required = false) Integer age){

        return userService.selectUser(name,age);
//        List<User> list = userService.list(new QueryWrapper<User>().eq(
//                name != null && !name.isEmpty(), "name",
//                SqlUtil.filterUnsafeSql(name)).eq(
//                age != null, "age", age));
//        return list;

    }

    @PostMapping("/user/update")
    public String updateUser(@RequestBody User user){
        userService.updateById(user);
        return SUCCESS;
    }

    @PostMapping("/user/delete")
    public String deleteUser(@RequestParam("id") Long id){
        userService.removeById(id);
        return SUCCESS;
    }


}
