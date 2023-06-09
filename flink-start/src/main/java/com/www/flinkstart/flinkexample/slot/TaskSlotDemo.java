package com.www.flinkstart.flinkexample.slot;

import com.www.flinkstart.flinkexample.datastream.User;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

import java.util.Arrays;
import java.util.List;

/**
 * @Description TaskSlotDemo
 * @Author 张卫刚
 * @Date Created on 2023/6/7
 */
public class TaskSlotDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(2);

        List<User> users = Arrays.asList(new User("zs", 20), new User("ls", 21), new User("ww", 22));
        DataStreamSource<User> source = env.fromCollection(users);
        WindowedStream<User, String, GlobalWindow> window = source.keyBy(User::getUsername).countWindow(1);

    }
}
