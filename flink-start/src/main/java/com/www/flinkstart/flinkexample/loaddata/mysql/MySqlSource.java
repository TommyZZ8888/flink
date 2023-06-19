package com.www.flinkstart.flinkexample.loaddata.mysql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @Description MySqlSource
 * @Author 张卫刚
 * @Date Created on 2023/6/1
 */
public class MySqlSource extends RichParallelSourceFunction<SourceTest> {

    Logger logger = LoggerFactory.getLogger(MySqlSource.class);

    Connection conn = null;
    PreparedStatement preparedStatement = null;
    ResultSet resultSet = null;
    private Boolean flag = true;

    String url = "jdbc:mysql://172.16.25.234/test?useUnicode=true&characterEncoding=UTF8&serverTimezone=Asia/Shanghai";

    @Override
    public void run(SourceContext<SourceTest> sourceContext) throws Exception {
        while (flag) {
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                logger.info("用户ID={},name={},age={},likes={},address={}",
                        resultSet.getInt("id"),
                        resultSet.getString("name"),
                        resultSet.getInt("age"),
                        resultSet.getString("likes"),
                        resultSet.getString("address"));
            }
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection(url,"root","123456");
        String sql = "select money,sex from test.test limit 10";
        preparedStatement = conn.prepareStatement(sql);
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        if (conn != null) {
            conn.close();
        }
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (resultSet != null) {
            resultSet.close();
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
