package com.www.flinkstart.flinkexample.loaddata.socket;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Description FlinkSocketSource
 * @Author 张卫刚
 * @Date Created on 2023/7/14
 */
public class FlinkSocketSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 9966);
        source.print();
        env.execute("socket");


//        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = env.socketTextStream("127.0.0.1", 8888,",")
//                .flatMap(new FlatMapFunction<String, String>() {
//                             @Override
//                             public void flatMap(String s, Collector<String> collector) throws Exception {
//
//                                 String[] split = s.split(",");
//                                 for (String s1 : split) {
//                                     collector.collect(s1);
//                                 }
//                             }
//                         }
//                ).map(new MapFunction<String, Tuple2<String, Integer>>() {
//                    @Override
//                    public Tuple2<String, Integer> map(String s) throws Exception {
//                        return Tuple2.of(s, 1);
//                    }
//                })
//                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
//                    @Override
//                    public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                        return stringIntegerTuple2.f0;
//                    }
//                })
//                .sum(1);
//
//        operator.print();
//        env.execute();

    }
}
