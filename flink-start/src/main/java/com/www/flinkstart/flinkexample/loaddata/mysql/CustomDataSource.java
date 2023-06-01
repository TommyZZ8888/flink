package com.www.flinkstart.flinkexample.loaddata.mysql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description CustomDataSource
 * @Author 张卫刚
 * @Date Created on 2023/6/1
 */
public class CustomDataSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        DataStreamSource<SourceTest> source = env.addSource(new MySqlSource());
        source.print();
        env.execute("mysql-source");
    }
}



