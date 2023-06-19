package com.www.flinkstart.flinkexample.window.time;

import cn.hutool.core.util.RandomUtil;
import com.www.flinkstart.flinkexample.Location;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @Description LocationSource
 * @Author 张卫刚
 * @Date Created on 2023/6/19
 */
public class LocationSource implements SourceFunction<Location> {

    Boolean flag = true;

    @Override
    public void run(SourceContext<Location> ctx) throws Exception {
        while (flag){
            int randomInt = RandomUtil.randomInt(1, 10);
            Location location = Location.builder()
                    .vehicleId(randomInt)
                    .plate("A000" + randomInt)
                    .color("黄")
                    .date(Integer.parseInt(LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE)))
                    .gpsSpeed(RandomUtil.randomInt(90, 100))
                    .limitSpeed(RandomUtil.randomInt(80, 90))
                    .devTime(System.currentTimeMillis() - RandomUtil.randomInt(5, 30) * 1000L)
                    .build();
            ctx.collect(location);
            Thread.sleep(2000);
        }
    }

    @Override
    public void cancel() {
flag = false;
    }
}
