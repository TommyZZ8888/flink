package com.www.flinkkafka.flink.utils;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @Description KafkaUtils
 * @Author 张卫刚
 * @Date Created on 2023/7/20
 */
public class KafkaUtils{

    //kafka地址
    public final static String server_ip = "localhost:9092";

    /**
     * 私有静态方法，创建Kafka生产者
     * @return KafkaProducer
     */
    public static KafkaProducer<String,String> createProducer(){
        //生产者配置
        Properties properties = new Properties();
        properties.put("bootstrap.servers",server_ip);
        properties.put("acks","all");
        //消息发送重试次数
        properties.put("retries",0);
        properties.put("batch.size",0);
        //提交延迟
        properties.put("linger.ms",1);
        properties.put("buffer.memory",33554432);
        //Kafka提供的序列化和反序列化类
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //创建KafkaProducer; ctrl+p可以查看传什么类型的参数，
        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);
        return producer;
    }

    /**
     * 私有静态方法，创建Kafka消费者
     * @param groupid 分组名称
     * @return KafkaComsumer
     */
    public static KafkaConsumer<String,String> createConsumer(String groupid){
        //消费者配置
        Properties properties = new Properties();
        properties.put("bootstrap.servers",server_ip);
        //分组名称
        properties.put("group.id",groupid);
        //每次拉取一百条，一条条消费，当然是具体业务状况设置
        properties.put("max.poll.records",100);
        properties.put("enable.auto.commit",true);
        properties.put("auto.offset.reset","earliest");
        //Kafka提供的序列化和反序列化类
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        //创建KafkaConsumer; ctrl+p可以查看传什么类型的参数
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(properties);
        return consumer;
    }

    /**
     * 传入kafka约定的topicName,json格式字符串，发送给kafka(集群)
     * @param topicName 交换机
     * @param jsonMessage 消息体
     */
    public static void sendMsg(String topicName,String jsonMessage){
        //调用创建生产者的方法，创建生产者
        KafkaProducer<String,String> producer = createProducer();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss");
        //调用发送消息的方法
        producer.send(new ProducerRecord<String,String>(topicName,0,sdf.format(System.currentTimeMillis()),jsonMessage));
        //关闭
        producer.close();
    }

    /**
     * 传入kafka约定的topicName,groupid
     * 注意数据掉失，消费未保存，所以获取数据结合业务保存来
     * @param topicName
     * @param groupid
     */
    public static ConsumerRecords<String,String> getMsg(String topicName, String groupid){
        //调用方法，创建一个消费者
        KafkaConsumer<String,String> consumer = createConsumer(groupid);
        List<PartitionInfo> ps = consumer.partitionsFor(topicName);
        consumer.subscribe(Collections.singletonList(topicName));
        ConsumerRecords<String,String> consumerRecords = consumer.poll(1000);
        consumer.commitAsync();
        //关闭
        consumer.close();
        return consumerRecords;
    }

}