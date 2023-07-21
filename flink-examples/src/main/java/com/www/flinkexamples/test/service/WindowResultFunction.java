package com.www.flinkexamples.test.service;

import com.www.flinkexamples.test.domain.ItemViewCount;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Description WindowResultFunction
 * @Author 张卫刚
 * @Date Created on 2023/6/16
 */
public class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {

    @Override
    public void apply(Long aLong, TimeWindow window, java.lang.Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
        while (input.iterator().hasNext()) {
            Long next = input.iterator().next();
            out.collect(new ItemViewCount(aLong, window.getEnd(), next));
        }
    }
}
