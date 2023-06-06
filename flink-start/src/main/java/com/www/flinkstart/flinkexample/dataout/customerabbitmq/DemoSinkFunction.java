package com.www.flinkstart.flinkexample.dataout.customerabbitmq;

import com.www.flinkstart.flinkexample.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

/**
 * @Description DemoSinkFunction
 * @Author 张卫刚
 * @Date Created on 2023/6/6
 */
public class DemoSinkFunction extends DataRichSinkFunction<String>{


    public DemoSinkFunction(RabbitMqSinkProperties rabbitMqSinkProperties) {
        super(rabbitMqSinkProperties);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        System.out.println(LocalDateTime.now() + "发送数据：" + value);
        channel.basicPublish(rabbitMqSinkProperties.getExchange(), "", null, value.getBytes(StandardCharsets.UTF_8));
    }
}
