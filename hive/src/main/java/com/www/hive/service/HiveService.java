package com.www.hive.service;

import com.www.hive.utils.HiveJDBC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @Description HIveService
 * @Author 张卫刚
 * @Date Created on 2023/7/17
 */
@Service
public class HiveService {
    static Logger logger = LoggerFactory.getLogger(HiveService.class);


    public static void save() throws SQLException {
        String sql = "insert into student(name, age, comment) VALUES ('zs',11,'zs_comment'),('ls',11,'ls_comment')";
        HiveJDBC.statement.execute(sql);
    }

    public static void init() throws Exception {
        try (ResultSet resultSet = HiveJDBC.connection.createStatement().executeQuery("select * from student")) {
            while (resultSet.next()) {
                logger.info("用户name={},age={},comment={}",
                        resultSet.getInt("name"),
                        resultSet.getString("age"),
                        resultSet.getInt("comment"));
                System.out.println(resultSet.getString("name"));
            }
        }
    }


    public static void main(String[] args) throws SQLException {
        HiveService.save();
    }

}
