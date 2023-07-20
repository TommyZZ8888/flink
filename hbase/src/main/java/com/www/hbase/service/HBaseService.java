package com.www.hbase.service;


import com.www.hbase.domain.Userinfos;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description HBaseService
 * @Author 张卫刚
 * @Date Created on 2023/7/18
 */
@Service
public class HBaseService {
    @Autowired
    private Connection hbaseConnection;

    //插入数据
    public void insert(Userinfos user){
        try {
            //获取数据库中的表
            Table table = null;
            table = hbaseConnection.getTable(TableName.valueOf("mydemo:userinfos"));
            //准备一行数据
            Put line = new Put(user.getUserid().getBytes());
            line.addColumn("base".getBytes(),"username".getBytes(),user.getUsername().getBytes());
            line.addColumn("base".getBytes(),"birthday".getBytes(),user.getBirthday().getBytes());
            //将数据插入数据库
            table.put(line);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //根据rowkey获取数据
    public Userinfos findByRowkey(String rowkey){
        Table table = null;
        Result result = null;
        Userinfos us = null;
        //获取hbase中的表
        try {
            table = hbaseConnection.getTable(TableName.valueOf("mydemo:userinfos"));
            //按照rowkey获取数据
            Get get = new Get(rowkey.getBytes());
            result = table.get(get);
            us = Userinfos.builder().username(new String(result.getValue("base".getBytes(),"username".getBytes()))).build();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return us;
    }


    //代码逻辑没问题  在海量数据下此方法不能使用 内存不足 会花费大量时间下载数据
    public List<Userinfos> findAll(){
        List<Userinfos> list = new ArrayList<Userinfos>();
        Table table = null;
        try {
            table = hbaseConnection.getTable(TableName.valueOf("mydemo:userinfos"));
            Scan scan = new Scan();
            ResultScanner rs = table.getScanner(scan);
            Result result = null;
            while ((result =rs.next())!= null){
                list.add(Userinfos.builder().username(new String(result.getValue("base".getBytes(),"username".getBytes()))).build());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    //根据username包含某字查询信息
    public List<Userinfos> findUserByPrefixName(String prefixName){
        Table table = null;
        List<Userinfos> list = new ArrayList<>();
        try {
            table = hbaseConnection.getTable(TableName.valueOf("mydemo:userinfos"));
            Scan scan = new Scan();
            //hbase中操纵命令：scan 'mydemo:userinfos',{FILTER=>"SingleColumnValueFilter('base','username',=,'substring:zhang')"}
            SingleColumnValueFilter vf = new SingleColumnValueFilter(
                    "base".getBytes(),
                    "username".getBytes(),
                    CompareFilter.CompareOp.EQUAL,
                    new SubstringComparator(prefixName)
            );
            scan.setFilter(vf);
            ResultScanner scanner = table.getScanner(scan);

            Result rs = null;
            while ((rs=scanner.next()) != null){
                list.add(Userinfos.builder().username(new String(rs.getValue("base".getBytes(),"username".getBytes()))).build());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }
}
