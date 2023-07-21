package com.www.flinkexamples.test.demoTest;


import com.www.flinkexamples.test.domain.ItemViewCount;
import com.www.flinkexamples.test.domain.UserBehavior;
import com.www.flinkexamples.test.service.CountAgg;
import com.www.flinkexamples.test.service.ProcessResultFunction;
import com.www.flinkexamples.test.service.WindowResultFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;


@Slf4j
public class ApplicationDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("D:\\test\\items\\github\\flink\\flink-examples\\src\\main\\resources\\UserBehavior.csv");
        SingleOutputStreamOperator<String> process = streamSource.map(line -> {
                    String[] data = line.split(",");
                    UserBehavior userBehavior = new UserBehavior(Long.parseLong(data[0]), Long.parseLong(data[1]), Long.parseLong(data[2]), data[3], Long.parseLong(data[4]));
                    log.info(userBehavior.toString());
                    return userBehavior;
                }).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)))
                .filter(userBehavior -> "pv".equals(userBehavior.getBehavior()))
                .keyBy(UserBehavior::getItemId)
                .window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(5)))
                .aggregate(new CountAgg(), new WindowResultFunction())
                .keyBy(ItemViewCount::getWindowEnd)
                .process(new ProcessResultFunction(3));

        process.print();
        env.execute();
    }
}
