package com.www.flinkstart.flinkexample.dataout.customredis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.www.flinkstart.flinkexample.Test;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.springframework.beans.factory.annotation.Autowired;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @Description FlinkSink4_Redis
 * @Author 张卫刚
 * @Date Created on 2023/6/5
 */
public class FlinkSink4_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        DataStreamSource<Test> streamSource = env.addSource(new MyRedisSource());
        streamSource.addSink(new MyRedisSink());
        env.execute("redis-sink");
    }


    public static class MyRedisSource implements SourceFunction<Test> {

        @Override
        public void run(SourceContext<Test> ctx) throws Exception {
            while (true) {
                long id = System.currentTimeMillis() / 1000;
                Test test = new Test();
                test.setTestId(String.valueOf(id));
                ctx.collect(test);
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {

        }
    }


    public static class MyRedisSink extends RichSinkFunction<Test> {

        private transient static JedisPool jedisPool = null;
        private transient static Jedis jedis = null;

        @Autowired
        private ObjectMapper objectMapper;


        static {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPool = new JedisPool(jedisPoolConfig,
                    "172.16.25.234",
                    6379,
                    3000,
                    "123456",
                    "redis");
        }


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            while (jedis == null) {
                jedis = jedisPool.getResource();
                Thread.sleep(100);
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (jedis != null) {
                jedis.close();
            }
            if (jedisPool != null) {
                jedisPool.close();
            }
        }

        @Override
        public void invoke(Test value, Context context) throws Exception {
            jedis.set(value.createTime + value.testId, objectMapper.writeValueAsString(value));
        }
    }
}




