package com.www.flinkstart.flinkexample.datastream;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @Description DataStream算子简单demo
 * @Author 张卫刚
 * @Date Created on 2023/6/2
 */
public class DataStreamDemo {
    public static void main(String[] args) throws Exception {
        coGroupFunction();
    }

    /**
     * map
     */
    public static void mapFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //设置并行度1 便于观察
        env.setParallelism(1);
        List<String> list = Arrays.asList("a", "b", "c");
        DataStreamSource<String> streamSource = env.fromCollection(list);
        SingleOutputStreamOperator<String> map = streamSource.map(String::toUpperCase).returns(Types.STRING);
        map.print();
        env.execute();
    }


    /**
     * flatMap
     * 可以将数据进行摊平化处理 例如 原本每一个元素都是集合或者数数组，我们使用FlatMap后，可以将（集合，数组）进行再次拆解取出其中的数据，再新组合为集合
     */
    public static void flatMapFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //设置并行度1 便于观察
        env.setParallelism(1);
        List<String> list = Arrays.asList("a", "b", "c");
        List<String> list1 = Arrays.asList("liu", "guan", "zhang");
        List<List<String>> originalData = new ArrayList<>();
        originalData.add(list);
        originalData.add(list1);
        DataStreamSource<List<String>> listDataStreamSource = env.fromCollection(originalData);
        SingleOutputStreamOperator<String> map = listDataStreamSource
                .flatMap((FlatMapFunction<List<String>, String>) (value, collector) -> value.forEach(collector::collect))
                .returns(Types.STRING);
        map.print();
        env.execute();
    }


    /**
     * filter
     */
    public static void filterFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //设置并行度1 便于观察
        env.setParallelism(1);

        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        DataStreamSource<Integer> streamSource = env.fromCollection(list);
        SingleOutputStreamOperator<Integer> map = streamSource.filter(value -> value > 5);
        map.print();
        env.execute();
    }


    /**
     * keyBy
     * 分组
     */
    public static void keyByFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //设置并行度1 便于观察
        env.setParallelism(1);

        List<User> users = Arrays.asList(new User("张三", 20), new User("李四", 21),
                new User("王五", 22), new User("李二", 23));
        DataStreamSource<User> streamSource = env.fromCollection(users);
        KeyedStream<User, String> map = streamSource.keyBy(user -> user.username);
        map.print();
        env.execute();
    }

    /**
     * max min sum reduce
     */
    public static void reduceFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //设置并行度1 便于观察
        env.setParallelism(1);

        List<User> users = Arrays.asList(new User("张三", 20), new User("李四", 21),
                new User("王五", 22), new User("张三", 23), new User("麻子", 24));
        DataStreamSource<User> streamSource = env.fromCollection(users);
        //输出了分组后（KeyBy） 每一个分组组中年龄最大的数据
//        SingleOutputStreamOperator<User> map = streamSource.keyBy(user -> user.username).max("age");
        //输出了分组后（KeyBy） 每一个分组组中年龄之和
//        SingleOutputStreamOperator<User> map = streamSource.keyBy(user -> user.username).sum("age");
        //reduce 有归约的意思，也可以将数据进行求和以及其余汇总处理
        SingleOutputStreamOperator<User> map = streamSource.keyBy(user -> user.username)
                .reduce((user1, user2) -> new User(user1.username, user1.age + user2.age));
        map.print();
        env.execute();
    }


    /**
     * union
     * 联合算子， 使用此算子，可对多个数据源进行合并操作（数据源数据必须类型必须相同），其可合并多个,合并后可直接对数据进行处理 （计算或输出）
     */
    public static void unionFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //设置并行度1 便于观察
        env.setParallelism(1);

        DataStreamSource<String> streamSource1 = env.fromElements("zs1", "ls1", "ww1");
        DataStreamSource<String> streamSource2 = env.fromElements("zs2", "ls2", "ww2");
        DataStreamSource<String> streamSource3 = env.fromElements("zs3", "ls3", "ww3");
        DataStream<String> unionStream = streamSource1.union(streamSource2, streamSource3);

        SingleOutputStreamOperator<String> map = unionStream.map(String::toUpperCase).returns(Types.STRING);
        map.print();
        env.execute();
    }

    /**
     * connect
     * connect与union算子一样,都可以进行数据源合并处理，但与union不同的是，connect 可以合并不同类型的数据源
     * 但最多只能合并两个数据流，且合并后无法直接操作（计算 输出）,需要对连接流进行数据处理（选择最终合并后的数据类型，不符合最终数据类型的转换）
     */
    public static void connectFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        DataStreamSource<String> streamSource1 = env.fromElements("zs", "ls", "ww");
        DataStreamSource<Integer> streamSource2 = env.fromElements(1, 2, 3, 4, 5, 6);
        ConnectedStreams<String, Integer> connectedStreams = streamSource1.connect(streamSource2);

        SingleOutputStreamOperator<String> map = connectedStreams.map(new CoMapFunction<String, Integer, String>() {
            @Override
            public String map1(String s) throws Exception {
                return s + " is string";
            }

            @Override
            public String map2(Integer value) throws Exception {
                return value + " is Integer but now convert string";
            }
        });
        map.print();
        env.execute();
    }


    /**
     * 过程函数，又叫低阶处理函数 （后续与window对比讲解），数据流中的每一个元素都会经过process
     */
    public static void processFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.fromElements("zs", "ls", "ww");
        SingleOutputStreamOperator<String> process = streamSource.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                collector.collect(s.toUpperCase() + ">>>");
            }
        });

        process.print();
        env.execute();
    }


    /**
     * side outputs(原split，select)
     * 1.12中split函数已过期并移除，已采用 Side Outputs结合 process方法处理
     * 将一个流 分为多个流
     */
    public static void sideOutputsFunction() throws Exception {
        String john = "john";
        String lucy = "lucy";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.fromElements("john.a", "john.b", "lucy.zs", "lucy.ls");
        // 定义拆分后的侧面输出标识 （名字 元素类型）
        OutputTag<String> johnTag = new OutputTag<>(john, TypeInformation.of(String.class));
        OutputTag<String> lucyTag = new OutputTag<>(lucy, TypeInformation.of(String.class));

        SingleOutputStreamOperator<String> source = streamSource.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                // 自定义数据归属判断 ex:以john打头数据则输出到johnTag，以lucy打头数据则输出到lucyTag
                if (value.startsWith(john)) {
                    context.output(johnTag, value);
                }
                if (value.startsWith(lucy)) {
                    context.output(lucyTag, value);
                }
            }
        });

        DataStream<String> johnStream = source.getSideOutput(johnTag);
        DataStream<String> lucyStream = source.getSideOutput(lucyTag);
        johnStream.print(john);
        lucyStream.print(lucy);

        env.execute("side-process");
    }


    /**
     * window
     * 其实际作用与Process作用类似，但相比Process（process 每个元素都会执行处理）
     * window对数据处理时机选择更加丰富 例如可以根据时间触发数据处理，根据事件时间触发数据处理，根据数量触发数据处理
     */
    public static void windowFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        DataStreamSource<Long> streamSource = env.fromSequence(1, 10);
        // 每两个元素 计算一次结果
        AllWindowedStream<Long, GlobalWindow> windowedStream = streamSource.countWindowAll(2);
        SingleOutputStreamOperator<Long> apply = windowedStream.apply(new RichAllWindowFunction<Long, Long, GlobalWindow>() {
            @Override
            public void apply(GlobalWindow globalWindow, Iterable<Long> iterable, Collector<Long> collector) throws Exception {
                Long sum = StreamSupport.stream(iterable.spliterator(), false)
                        .reduce(Long::sum).orElse(99999L);
                collector.collect(sum);
            }
        });

        apply.print();
        env.execute();
    }


    /**
     * CoGroup
     * 将两个流进行关联，关联不上的数据仍会保留下来
     */
    public static void coGroupFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        List<User> users = Arrays.asList(new User("zs", 20), new User("ls", 21), new User("ww", 22));
        List<Student> students = Arrays.asList(new Student("zs", "zz"), new Student("ls", "ll"), new Student("zl", "xx"));
        DataStreamSource<User> userDataStreamSource = env.fromCollection(users);
        DataStreamSource<Student> studentDataStreamSource = env.fromCollection(students);

        DataStream<Tuple3<String, String, Integer>> dataStream = userDataStreamSource.coGroup(studentDataStreamSource)
                //关联条件中user中name=student中name
                .where(u -> u.username).equalTo(t -> t.username)
                //使用时间窗口滚动 1s计算一次
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .apply(new CoGroupFunction<User, Student, Tuple3<String, String, Integer>>() {
                    @Override
                    public void coGroup(Iterable<User> first, Iterable<Student> second, Collector<Tuple3<String, String, Integer>> collector) throws Exception {
                        Iterator<User> userIterator = first.iterator();
                        Iterator<Student> studentIterator = second.iterator();
                        if (userIterator.hasNext() && studentIterator.hasNext()) {
                            User user = userIterator.next();
                            Student student = studentIterator.next();
                            collector.collect(Tuple3.of(user.username, student.nickName, user.age));
                        } else if (userIterator.hasNext() || studentIterator.hasNext()) {
                            if (userIterator.hasNext()) {
                                while (userIterator.hasNext()) {
                                    User user = userIterator.next();
                                    collector.collect(Tuple3.of(user.username, "no nickName", user.age));
                                }
                            }
                            if (studentIterator.hasNext()) {
                                while (studentIterator.hasNext()) {
                                    Student student = studentIterator.next();
                                    collector.collect(Tuple3.of(student.username, student.nickName, -1));
                                }
                            }
                        }
                    }
                });

        dataStream.print();
        env.execute();
    }


    public void chainCalculate() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        DataStreamSource<String> streamSource = env.socketTextStream("xx", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = streamSource.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        for (String s1 : s.split(",")) {
                            collector.collect(s1);
                        }
                    }
                }).filter(s -> !s.equals("sb"))
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String s) throws Exception {
                        return s.toUpperCase();
                    }
                }).map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        return Tuple2.of(s, 1);
                    }
                }).keyBy(tp -> tp.f0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                        return Tuple2.of(t1.f0, t1.f1 + stringIntegerTuple2.f1);
                    }
                });
        result.print();
        env.execute();
    }
}
