package com.www.flinkstart.flinkexample.loaddata.rabbitmq;

import com.rabbitmq.client.AMQP;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @Description FlinkRabbitMqSource
 * @Author 张卫刚
 * @Date Created on 2023/6/1
 */
public class FlinkRabbitMqSource extends RMQSource {


    private String exchangeName;

    /**
     * 设置消息队列，队列绑定到交换机
     * @throws IOException
     */
    @Override
    protected void setupQueue() throws IOException {
        AMQP.Queue.DeclareOk result = channel.queueDeclare(queueName, true, false, false, null);
        channel.queueBind(result.getQueue(),exchangeName,"*");
    }

    public FlinkRabbitMqSource(RMQConnectionConfig rmqConnectionConfig, String queueName, String exchangeName) {
        super(rmqConnectionConfig,queueName,new SimpleStringSchema(StandardCharsets.UTF_8));
        this.exchangeName = exchangeName;
    }
}
