package com.www.flinkstart.flinkexample.dataout.customerabbitmq;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description RabbitMqSinkProperties
 * @Author 张卫刚
 * @Date Created on 2023/6/6
 */

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RabbitMqSinkProperties {

    private String host;

    private int port;

    private String userName;

    private String password;

    private String exchange;
}
