package com.www.flinkexamples.test.service;

import com.www.flinkexamples.test.domain.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @Description MySqlSource
 * @Author 张卫刚
 * @Date Created on 2023/6/16
 */
public class MySqlSource extends RichParallelSourceFunction<UserBehavior> {

    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    Boolean flag = false;

    String url = "jdbc:mysql://172.16.25.234/austin?useUnicode=true&characterEncoding=UTF8&serverTimezone=Asia/Shanghai";


    @Override
    public void run(SourceContext<UserBehavior> sourceContext) throws Exception {
        while (flag) {
            rs = ps.executeQuery();
            while (rs.next()) {
                long userId = rs.getLong("user_id");
                long itemId = rs.getLong("item_id");
                long categoryId = rs.getLong("category_id");
                long timestamp = rs.getLong("timestamp");
                String behavior = rs.getString("behavior");
                UserBehavior userBehavior = new UserBehavior(userId, itemId, categoryId, behavior, timestamp);
                sourceContext.collect(userBehavior);
            }
            Thread.sleep(2000);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection(url, "root", "123456");
        String sql = "select * from user_behavior limit 10";
        ps = conn.prepareStatement(sql);
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        if (conn != null) {
            conn.close();
        }
        if (ps != null) {
            ps.close();
        }
        if (rs != null) {
            rs.close();
        }
    }

    @Override
    public void cancel() {

    }
}