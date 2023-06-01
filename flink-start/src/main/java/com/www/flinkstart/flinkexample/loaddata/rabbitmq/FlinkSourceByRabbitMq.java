package com.www.flinkstart.flinkexample.loaddata.rabbitmq;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

/**
 * @Description FlinkSourceByRabbitMq
 * @Author 张卫刚
 * @Date Created on 2023/6/1
 */
public class FlinkSourceByRabbitMq {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("172.16.25.234")
                .setPort(5672)
                .setUserName("guest")
                .setPassword("guest")
                .setVirtualHost("/")
                .build();
        DataStreamSource dataStreamSource = env.addSource(new FlinkRabbitMqSource(connectionConfig, "queueName", "exchangeName"));
        dataStreamSource.print();
        env.execute();
    }
}
