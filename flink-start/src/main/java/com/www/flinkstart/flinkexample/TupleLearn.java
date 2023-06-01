package com.www.flinkstart.flinkexample;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Arrays;


/**
 * @Description TupleTest
 * @Author 张卫刚
 * @Date Created on 2023/5/30
 */
public class TupleLearn {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        //lambda简化后
        DataStreamSource<String> streamSource = env.fromElements("q", "w", "e", "r");
        SingleOutputStreamOperator<Tuple2<String, Integer>> returns = streamSource
                .flatMap((FlatMapFunction<String, String>) (s, collector) -> Arrays.stream(s.split(",")).forEach(collector::collect))
                .returns(Types.STRING)
                .filter(s -> !"zsls".equals(s))
                .map(String::toUpperCase).returns(Types.STRING)
                .map(s -> Tuple2.of(s, 1)).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tp -> tp.f0)
                .reduce((t2, t1) -> Tuple2.of(t1.f0, t1.f1 + t2.f1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        //lambda简化前
//        SingleOutputStreamOperator<String> flatMapStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public void flatMap(String s, Collector<String> collector) throws Exception {
//                for (String s1 : s.split(",")) {
//                    collector.collect(s1);
//                }
//            }
//        });
//        SingleOutputStreamOperator<String> filterStream = flatMapStream.filter(s -> !s.equals("zsls"));
//        SingleOutputStreamOperator<String> mapStream = filterStream.map(new MapFunction<String, String>() {
//            @Override
//            public String map(String s) throws Exception {
//                return s.toUpperCase();
//            }
//        });
//        DataStream<Tuple2<String, Integer>> map = mapStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> map(String s) throws Exception {
//                return Tuple2.of(s, 1);
//            }
//        });
//        KeyedStream<Tuple2<String, Integer>, String> groupStream = map.keyBy(tp -> tp.f0);
//        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = groupStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t2, Tuple2<String, Integer> t1) throws Exception {
//                return Tuple2.of(t1.f0, t1.f1 + t2.f1);
//            }
//        });

        returns.print();
        env.execute();
    }
}
