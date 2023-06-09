package com.www.flinkstart.flinkexample.window;

import com.www.flinkstart.flinkexample.datastream.User;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @Description SpeedAlarmWindow
 * @Author 张卫刚
 * @Date Created on 2023/6/9
 */
public class SpeedAlarmWindow extends RichWindowFunction<User, Object, String, GlobalWindow> {

    @Override
    public void apply(String s, GlobalWindow window, Iterable<User> input, Collector<Object> out) throws Exception {
        Iterator<User> iterator = input.iterator();
        while (iterator.hasNext()) {
            User user = iterator.next();
            out.collect(user.username);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("我进来啦");
    }

    @Override
    public void close() throws Exception {
        super.close();
        System.out.println("我又出去啦");
    }
}
