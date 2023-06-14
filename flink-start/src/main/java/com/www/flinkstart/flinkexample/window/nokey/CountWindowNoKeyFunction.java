package com.www.flinkstart.flinkexample.window.nokey;

import com.www.flinkstart.flinkexample.datastream.User;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.UUID;

/**
 * @Description CountWindowNoKeyFunction
 * @Author 张卫刚
 * @Date Created on 2023/6/14
 */
public class CountWindowNoKeyFunction extends RichAllWindowFunction<User,String, GlobalWindow> {

    @Override
    public void apply(GlobalWindow window, Iterable<User> values, Collector<String> out) throws Exception {
        for (User value : values) {
            out.collect(value.username);
        }
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        UUID uuid = UUID.randomUUID();
        System.out.println(uuid+"窗口打开了");
    }
}
