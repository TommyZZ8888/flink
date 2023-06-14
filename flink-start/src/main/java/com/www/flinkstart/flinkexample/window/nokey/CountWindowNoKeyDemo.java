package com.www.flinkstart.flinkexample.window.nokey;

import com.www.flinkstart.flinkexample.datastream.User;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

import java.util.Arrays;
import java.util.List;

/**
 * @Description 无key窗口
 * @Author 张卫刚
 * @Date Created on 2023/6/14
 */
public class CountWindowNoKeyDemo {
    public static void main(String[] args) throws Exception {
        /*
         * 无key窗口，作用在非键控流基础之上，你的原始流不会拆分为多个逻辑流，并且所有窗口逻辑将由单个任务（即并行度只会为1）执行
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(4);

        List<User> users = Arrays.asList(new User("zs", 21), new User("ls", 22),
                new User("ww", 23), new User("zs", 24));
        DataStreamSource<User> source = env.fromCollection(users);
        AllWindowedStream<User, GlobalWindow> windowedStream = source.keyBy(User::getUsername).countWindowAll(2);
        SingleOutputStreamOperator<String> apply = windowedStream.apply(new CountWindowNoKeyFunction());

        apply.print();
        env.execute();
    }
}
