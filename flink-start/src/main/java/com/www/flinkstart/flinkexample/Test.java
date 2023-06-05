package com.www.flinkstart.flinkexample;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * @Description Test
 * @Author 张卫刚
 * @Date Created on 2023/6/5
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Test {

    public String testId;

    public String test2Id;

    public Long money;

    public String sex;

    public Date createTime;

    public Date updateTime;
}
