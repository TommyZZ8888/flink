package com.www.flinkexamples.test.service;

import com.www.flinkexamples.test.domain.ItemViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;

/**
 * @Description ProcessFunction
 * @Author 张卫刚
 * @Date Created on 2023/6/16
 */
public class ProcessResultFunction extends KeyedProcessFunction<Long, ItemViewCount, String> {

    private int topSize = 0;


    public ProcessResultFunction(int topSize) {
        this.topSize = topSize;
    }

    private ListState<ItemViewCount> itemState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ListStateDescriptor<ItemViewCount> itemStateList = new ListStateDescriptor<>("itemState", ItemViewCount.class);
        itemState = getRuntimeContext().getListState(itemStateList);
    }

    @Override
    public void processElement(ItemViewCount value, KeyedProcessFunction<Long, ItemViewCount, String>.Context ctx, Collector<String> out) throws Exception {
        itemState.add(value);
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Long, ItemViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemState.get().iterator());
        itemViewCounts.sort((o1, o2) -> o2.getCount().compareTo(o1.getCount()));

        itemState.clear();
        StringBuilder sb = new StringBuilder();
        sb.append("==============================\n");
        sb.append("时间:").append(new Timestamp(timestamp - 1)).append("\n");
        for (int i = 0; i < Math.min(topSize, itemViewCounts.size()); i++) {
            ItemViewCount currentItem = itemViewCounts.get(i);

            sb.append("No").append(i + 1).append(":").append(" 商品ID=").append(currentItem.getItemId())
                    .append(" 浏览量=").append(currentItem.getCount()).append("\n");
        }
        sb.append("====================================\n");
        Thread.sleep(1000);
        out.collect(sb.toString());

    }
}
