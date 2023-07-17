package com.www.hive.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * @Description:
 * @Author: 张文Uncle
 * @Date: 2021-03-17 11:08
 **/
public class HiveJDBC {

    public static Connection connection = null;
    public static Statement statement = null;


    static {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            connection = DriverManager.getConnection("jdbc:hive2://localhost:10000/hive_test");
            statement = connection.createStatement();
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }

    }




}
