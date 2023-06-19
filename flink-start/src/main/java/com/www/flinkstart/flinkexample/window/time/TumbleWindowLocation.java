package com.www.flinkstart.flinkexample.window.time;

import com.www.flinkstart.flinkexample.Location;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.UUID;

/**
 * @Description TumbleWindowLocation
 * @Author 张卫刚
 * @Date Created on 2023/6/19
 */
public class TumbleWindowLocation extends RichWindowFunction<Location, List<Location>, Integer, TimeWindow> {

    public TumbleWindowLocation(){}
    String uuid;

    @Override
    public void apply(Integer integer, TimeWindow window, Iterable<Location> locations, Collector<List<Location>> out) throws Exception {
        out.collect(Lists.newArrayList(locations));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        uuid = UUID.randomUUID().toString();
        System.out.println(uuid + "窗口打开了");
    }

    @Override
    public void close() throws Exception {
        super.close();
        System.out.println("窗口关闭了");
    }
}
