package com.www.flinkkafka.flink.readMemToFile;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;

/**
 * @Description 提取当前可用内存字节数
 * @Author 张卫刚
 * @Date Created on 2023/7/20
 */
public class MemoryUsageExtractor {

    private static OperatingSystemMXBean mxBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    /**
     * Get current free memory size in bytes
     * @return  free RAM size
     */
    public static long currentFreeMemorySizeInBytes() {
        return mxBean.getFreePhysicalMemorySize();
    }
}
