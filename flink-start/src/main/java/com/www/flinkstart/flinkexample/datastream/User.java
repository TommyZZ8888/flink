package com.www.flinkstart.flinkexample.datastream;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


/**
 * @Description User
 * @Author 张卫刚
 * @Date Created on 2023/6/2
 */

@AllArgsConstructor
@NoArgsConstructor
@Data
public class User {
    public String username;

    public Integer age;


}
