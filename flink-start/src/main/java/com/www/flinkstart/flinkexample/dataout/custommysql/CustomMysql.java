package com.www.flinkstart.flinkexample.dataout.custommysql;

import com.www.flinkstart.flinkexample.Test;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Date;

/**
 * @Description CustomMysql
 * @Author 张卫刚
 * @Date Created on 2023/6/5
 */
public class CustomMysql {
    public static void main(String[] args) throws Exception {
        //准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 加载数据源
        DataStreamSource<Test> streamSource = env.addSource(new MySource());
        //todo  transformation(数据处理)

        // 数据输出
        streamSource.addSink(new CustomMysqlSink());
        // 程序执行
        env.execute("learn-mysql-sink");
    }


    /**
     * 自定义数据源
     */
    public static class MySource implements SourceFunction<Test> {
        @Override
        public void run(SourceContext<Test> ctx) throws Exception {
            while (true) {
                long id = System.currentTimeMillis();
                Test vehicleAlarm = new Test();
                ctx.collect(vehicleAlarm);
                Thread.sleep(10000);
            }
        }

        @Override
        public void cancel() {
        }
    }


}
