package com.www.hbase.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.context.annotation.Scope;

/**
 * @Description Userinfos
 * @Author 张卫刚
 * @Date Created on 2023/7/18
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Scope("prototype")
public class Userinfos {
    private String userid;
    private String username;
    private String birthday;
}
