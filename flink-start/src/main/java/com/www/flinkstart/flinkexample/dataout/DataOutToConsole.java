package com.www.flinkstart.flinkexample.dataout;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description 控制台打印
 * @Author 张卫刚
 * @Date Created on 2023/6/5
 */
public class DataOutToConsole {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        DataStreamSource<Long> streamSource = env.fromSequence(1, 10);
        streamSource.print("normal print");

        streamSource.printToErr("error print");
        env.execute();
    }
}
