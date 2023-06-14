package com.www.flinkstart.flinkexample.window.key;

import com.www.flinkstart.flinkexample.datastream.User;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.UUID;

/**
 * @Description countWindowKeyFunction
 * @Author 张卫刚
 * @Date Created on 2023/6/14
 */
public class CountWindowKeyFunction extends RichWindowFunction<User, String, String, GlobalWindow> {


    @Override
    public void apply(String s, GlobalWindow window, Iterable<User> users, Collector<String> out) throws Exception {
        for (User user : users) {
            if ("zs".equals(user.username)) {
                out.collect(user.username);
            }
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        UUID uuid = UUID.randomUUID();
        System.out.println(uuid + "窗口打开了");
    }
}
