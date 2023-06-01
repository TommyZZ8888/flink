package com.www.flinkstart.flinkexample.loaddata.mysql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Description LoadDataSource
 * @Author 张卫刚
 * @Date Created on 2023/6/1
 */
public class LoadDataSource {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        //1 元素
        //DataStreamSource<String> stringDataStreamSource = env.fromElements("1", "2", "3");

        //2 集合
        //DataStreamSource<String> stringDataStreamSource = env.fromCollection(Arrays.asList("1", "2", "3"));

        //3 范围
        //DataStreamSource<Long> longDataStreamSource = env.fromSequence(1L, 10L);

        //5 socket
        //env.socketTextStream("ip",8080);

        //4
//        DataStreamSource<String> stringDataStreamSource = env.readTextFile("D:\\test\\items\\github\\flink\\flink-start\\src\\main\\resources\\static\\file\\file.md");
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("D:\\test\\items\\github\\flink\\flink-start\\src\\main\\resources\\static\\file\\");

        stringDataStreamSource.print();
        env.execute();
    }
}
