package com.www.flinkstart.flinkexample.window.time;

import com.alibaba.fastjson2.JSON;
import com.www.flinkstart.flinkexample.Location;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Description AllowedLatenessDemo
 * @Author 张卫刚
 * @Date Created on 2023/6/20
 */
public class AllowedLatenessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(4);

        DataStreamSource<Location> dataStreamSource = env.addSource(new LocationSource());
        SingleOutputStreamOperator<Location> watermarks = dataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Location>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> event.getDevTime()));

        SingleOutputStreamOperator<String> apply = watermarks.keyBy(Location::getVehicleId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(5))
                .apply(new AlarmCalcWindow());

        apply.print();
        env.execute();
    }



    public static class AlarmCalcWindow extends RichWindowFunction<Location, String, Integer, TimeWindow> {
        MapState<String, Integer> mapState;

        @Override
        public void apply(Integer key, TimeWindow window, Iterable<Location> input, Collector<String> out) throws Exception {
            System.out.println(String.format("窗口执行--开始时间:%s-------结束时间%s", window.getStart(), window.getEnd()));
            //todo 迭代器元素根据时间排序
            for (Location location : input) {
                String s = location.getPlate() + location.getColor();
                Integer value = mapState.get(s);
                if (value == null) {
                    mapState.put(s, 1);
                } else {
                    mapState.put(s, mapState.get(s) + 1);
                }
                out.collect(JSON.toJSONString(location));
                System.out.println(mapState.get(location.getPlate() + location.getColor()));
            }
        }

        @Override
        public void open(Configuration parameters) {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("test",
                    TypeInformation.of(String.class), TypeInformation.of(Integer.class)));
        }
    }
}
