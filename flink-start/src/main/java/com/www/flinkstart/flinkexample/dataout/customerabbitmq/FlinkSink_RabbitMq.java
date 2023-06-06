package com.www.flinkstart.flinkexample.dataout.customerabbitmq;


import cn.hutool.setting.dialect.Props;
import com.www.flinkstart.flinkexample.Test;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @Description FlinkRabbitMq
 * @Author 张卫刚
 * @Date Created on 2023/6/6
 */
public class FlinkSink_RabbitMq {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        DataStreamSource<Test> streamSource = env.addSource(new MyRabbitSource());
        //读取rabbitMq sink配置文件
        String path = "RabbitMqSink.properties";
        Props props = new Props(path);
        RabbitMqSinkProperties sinkProperties = RabbitMqSinkProperties.builder()
                .host(props.getStr("sink.rabbitmq.host"))
                .port(props.getInt("sink.rabbitmq.port"))
                .userName(props.getStr("sink.rabbitmq.username"))
                .password(props.getStr("sink.rabbitmq.password"))
                .exchange(props.getStr("sink.rabbitmq.exchange"))
                .build();

        SinkFunction demoSinkFunction = new DemoSinkFunction(sinkProperties);
        streamSource.addSink(demoSinkFunction);
        env.execute();
    }

    public static class MyRabbitSource implements SourceFunction<Test>{

        @Override
        public void run(SourceContext<Test> ctx) throws Exception {
            while (true) {
                long id = System.currentTimeMillis() / 1000;
                Test test = new Test();
                test.setTestId(String.valueOf(id));
                ctx.collect(test);
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {

        }
    }
}
