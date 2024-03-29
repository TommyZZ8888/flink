package com.www.flinkkafka.flink.readMemToFile;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @Description KafkaProducerTest
 * @Author 张卫刚
 * @Date Created on 2023/7/20
 */
public class KafkaProducerTest {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        int totalMessageCount = 20;
        for (int i = 0; i < totalMessageCount; i++) {
            String value = String.format("%d,%s,%d", System.currentTimeMillis(), "machine-1", currentMemSize());
            System.out.println(value);
            producer.send(new ProducerRecord<>("topic001", value), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        System.out.println("Failed to send message with exception " + exception);
                    }
                }
            });
            Thread.sleep(100L);
        }
        producer.close();
    }

    private static long currentMemSize() {
        return MemoryUsageExtractor.currentFreeMemorySizeInBytes();
    }

}
