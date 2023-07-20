package com.www.flinkkafka.flink.start;

import com.www.flinkkafka.flink.utils.KafkaUtils;

/**
 * @Description KafkaProducerService
 * @Author 张卫刚
 * @Date Created on 2023/7/20
 */
public class KafkaProducerService {
    public static void main(String[] args) {
        KafkaUtils.sendMsg("topic001","hello flink-kafka");
    }
}
