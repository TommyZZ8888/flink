package com.www.flinkstart.flinkexample.window;

import com.www.flinkstart.flinkexample.datastream.User;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import scala.tools.nsc.Global;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @Description CountWindowDemo
 * @Author 张卫刚
 * @Date Created on 2023/6/9
 */
public class CountWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        List<User> users = Arrays.asList(new User("zs", 21), new User("ls", 22), new User("ww", 23), new User("ww", 25),new User("zs",24));
        DataStreamSource<User> source = env.fromCollection(users);
        //countWindow(1) 设置为1，那么每来一个数据触发一次窗口计算，
        // 设置为2，则每相同的key出现两次，才会执行窗口计算
//        WindowedStream<User, String, GlobalWindow> windowedStream = source.keyBy(User::getUsername).countWindow(2);
//        SingleOutputStreamOperator<Object> apply = windowedStream.apply(new SpeedAlarmWindow());
//        apply.print();
        SingleOutputStreamOperator<Object> apply1 = source.keyBy(User::getUsername).countWindow(2, 2).apply(new SpeedAlarmWindow());
        apply1.print();

        env.execute();

    }
}
