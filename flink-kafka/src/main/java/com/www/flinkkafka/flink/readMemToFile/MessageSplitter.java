package com.www.flinkkafka.flink.readMemToFile;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Description MessageSplitter
 * @Author 张卫刚
 * @Date Created on 2023/7/20
 */
public class MessageSplitter implements FlatMapFunction<String, Tuple2<String,Long>> {
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Long>> collector) throws Exception {
        if (value!=null && value.contains(",")){
            String[] split = value.split(",");
            collector.collect(new Tuple2<>(split[1],Long.parseLong(split[2])));
        }
    }
}
