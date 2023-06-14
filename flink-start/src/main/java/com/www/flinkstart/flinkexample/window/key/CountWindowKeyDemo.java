package com.www.flinkstart.flinkexample.window.key;

import com.www.flinkstart.flinkexample.datastream.User;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

import java.util.Arrays;
import java.util.List;

/**
 * @Description 有key窗口：  键控流数据区别在于 原始流是否进行了keyBy()
 * @Author 张卫刚
 * @Date Created on 2023/6/12
 */
public class CountWindowKeyDemo {
    public static void main(String[] args) throws Exception {
        /*
         * 有key窗口，作用在键控流之上，持有相同key数据会进入同一窗口。
         * 有key窗口可以根据并行度设置开启多个窗口，引用同一键的所有元素始终都会被发送到同一并行任务中执行
         * （窗口计算会由多个subtask（根据并行度）执行），具有相同类型的key会进入同一个subtask中执行
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //有key窗口会根据并行度开启对应数量个窗口，比如并行度设置为4，那么只会开启4个窗口
        env.setParallelism(2);

        List<User> users = Arrays.asList(new User("zs", 20), new User("ls", 21),
                new User("ww", 22), new User("zs", 24));
        DataStreamSource<User> source = env.fromCollection(users);
        WindowedStream<User, String, GlobalWindow> countWindow = source.keyBy(User::getUsername).countWindow(2);
        SingleOutputStreamOperator<String> apply = countWindow.apply(new CountWindowKeyFunction());

        /*
         * 具有相同key的数据会进入同一个subtask中执行，但不代表一个subtask只会处理一个key的数据
         * 因为可能根据keyBy后存在超出并行度数量的key，那么超出并行度数量的key仍会根据一定规则选择一个subtask执行，且以后一直由那一个执行--
         * 只是说一个subtask在处理当前key时会阻塞，当一个key的数据处理完之后，才会处理另一个key的数据
         */
        apply.print();
        env.execute();
    }
}
