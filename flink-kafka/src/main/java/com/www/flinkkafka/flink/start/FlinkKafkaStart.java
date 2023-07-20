package com.www.flinkkafka.flink.start;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Description FlinkKafkaStart
 * @Author 张卫刚
 * @Date Created on 2023/7/20
 */
public class FlinkKafkaStart {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink");

        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("topic001", new SimpleStringSchema(), properties));
        SingleOutputStreamOperator<String> operator = dataStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return "flink" + value;
            }
        });
        operator.print();
        env.execute("kafka-flink-start");
    }
}
