package com.www.hbase.config;

import com.www.hbase.service.HBaseService;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * @Description HbaseConfig
 * @Author 张卫刚
 * @Date Created on 2023/7/18
 */

@Configuration
public class CustomerHbaseConfiguration {
    @Value("${hbase.zookeeper.quorum}")
    private String quorum;

    @Bean
    public org.apache.hadoop.conf.Configuration getConfig(){
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM,quorum);
        return conf;
    }
    //每次用户调用get方法获得一个新的数据库连接  可以考虑并发
    @Bean
    public Supplier<Connection> hbaseConnSupplier(){
        return this::hbaseConnect;
    }
    //获取数据库连接
    @Bean
    @Scope("prototype")
    public Connection hbaseConnect(){
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(getConfig());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
