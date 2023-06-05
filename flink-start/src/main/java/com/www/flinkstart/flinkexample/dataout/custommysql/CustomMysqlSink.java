package com.www.flinkstart.flinkexample.dataout.custommysql;

import com.www.flinkstart.flinkexample.Test;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @Description CustomMysqlSink
 * @Author 张卫刚
 * @Date Created on 2023/6/5
 */
public class CustomMysqlSink extends RichSinkFunction<Test> {
    Connection conn = null;
    PreparedStatement ps = null;
    String url = "jdbc:mysql://xx:3306/alarm-sc?useUnicode=true&characterEncoding=utf-8&useSSL=false";
    String username = "root";
    String password = "xx";


    @Override
    public void open(Configuration parameters) throws Exception {
       conn = DriverManager.getConnection(url,username,password);
    conn.setAutoCommit(false);
    }

    @Override
    public void close() throws Exception {
        if (conn!=null){
            conn.close();
        }
        if (ps!=null){
            ps.close();
        }
    }

    @Override
    public void invoke(Test value, Context context) throws Exception {
        String sql = "insert into test (`test_id`,`test2_id`,`money`,`sex`,`create_time`) " +
                "values(?,?,?,?,?)";
        ps = conn.prepareStatement(sql);
        ps.setString(1, value.testId);
        ps.setString(2, value.test2Id);
        ps.setLong(3, value.money);
        ps.setString(4, value.sex);
        ps.setDate(5, (Date) value.createTime);
        // 执行语句
        ps.execute();
        // 提交
        conn.commit();

    }
}
