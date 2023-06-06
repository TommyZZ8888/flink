package com.www.flinkstart.flinkexample.dataout.customerabbitmq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @Description 公共模板类 如果有多个mq-sink 直接继承此类即可
 * @Author 张卫刚
 * @Date Created on 2023/6/6
 */
public class DataRichSinkFunction<T> extends RichSinkFunction<T> {

    protected RabbitMqSinkProperties rabbitMqSinkProperties;

    protected Connection connection;

    protected Channel channel;

    public DataRichSinkFunction(RabbitMqSinkProperties rabbitMqSinkProperties){
        this.rabbitMqSinkProperties = rabbitMqSinkProperties;
    }


    /**
     * open中建立连接 这样就不用每次invoke的时候建立连接和关闭连接
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(rabbitMqSinkProperties.getHost());
        connectionFactory.setPort(rabbitMqSinkProperties.getPort());
        connectionFactory.setUsername(rabbitMqSinkProperties.getUserName());
        connectionFactory.setPassword(rabbitMqSinkProperties.getPassword());

        connection = connectionFactory.newConnection();

        channel = connection.createChannel();
        channel.exchangeDeclare(rabbitMqSinkProperties.getExchange(), BuiltinExchangeType.FANOUT,true);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        super.invoke(value, context);
    }
}
