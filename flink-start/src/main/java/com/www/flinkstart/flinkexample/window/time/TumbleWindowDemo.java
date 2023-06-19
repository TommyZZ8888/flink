package com.www.flinkstart.flinkexample.window.time;

import com.www.flinkstart.flinkexample.Location;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;
import java.util.List;

/**
 * @Description TumbleWindowDemo
 * @Author 张卫刚
 * @Date Created on 2023/6/19
 */
public class TumbleWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        DataStreamSource<Location> source = env.addSource(new LocationSource());

        WindowedStream<Location, Integer, TimeWindow> window = source
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <Location>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        // 设置事件时间为devTime属性
                        .withTimestampAssigner((event, timestamp) -> event.getDevTime())).keyBy(Location::getVehicleId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)));


        //每五秒，计算一次最近十秒的数据
//        WindowedStream<Location, Integer, TimeWindow> window = source.keyBy(Location::getVehicleId)
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)));

        //每五秒触发一次窗口计算逻辑
//        WindowedStream<Location, Integer, TimeWindow> window = source.keyBy(Location::getVehicleId)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)));
        SingleOutputStreamOperator<List<Location>> stream = window.apply(new TumbleWindowLocation());
        stream.print();
        env.execute();

    }
}
