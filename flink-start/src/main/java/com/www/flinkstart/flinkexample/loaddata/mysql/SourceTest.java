package com.www.flinkstart.flinkexample.loaddata.mysql;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description MySqlSource
 * @Author 张卫刚
 * @Date Created on 2023/6/1
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public  class SourceTest {

    private Long money;

    private String sex;
}
