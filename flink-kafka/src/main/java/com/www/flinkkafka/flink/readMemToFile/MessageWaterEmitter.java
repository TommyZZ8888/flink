package com.www.flinkkafka.flink.readMemToFile;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;


/**
 * @Description 根据kafka消息确定flink水位
 * @Author 张卫刚
 * @Date Created on 2023/7/20
 */
public class MessageWaterEmitter implements AssignerWithPunctuatedWatermarks<String> {
    public org.apache.flink.streaming.api.watermark.Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
        if (lastElement != null && lastElement.contains(",")) {
            String[] parts = lastElement.split(",");
            return new Watermark(Long.parseLong(parts[0]));
        }
        return null;
    }

    public long extractTimestamp(String element, long previousElementTimestamp) {
        if (element != null && element.contains(",")) {
            String[] parts = element.split(",");
            return Long.parseLong(parts[0]);
        }
        return 0L;
    }
}
