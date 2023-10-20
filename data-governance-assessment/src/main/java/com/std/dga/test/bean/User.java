package com.std.dga.test.bean;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName:User
 * Description:
 *
 * @date:2023/10/7 16:35
 * @author:STD
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class User {

    @TableId(value = "id",type = IdType.AUTO)
    private Long id;
    private String name;
    private Integer age;
}
