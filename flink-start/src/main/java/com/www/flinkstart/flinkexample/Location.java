package com.www.flinkstart.flinkexample;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description Location
 * @Author 张卫刚
 * @Date Created on 2023/6/19
 */

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Location {

    private int vehicleId;

    private String plate;

    private int date;

    private int gpsSpeed;

    private Long devTime;

    private String color;

    private int limitSpeed;
}
