package com.www.hbase.controller;

import com.www.hbase.domain.Userinfos;
import com.www.hbase.service.HBaseService;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

/**
 * @Description InitController
 * @Author 张卫刚
 * @Date Created on 2023/7/18
 */
@RestController
public class InitController {
    @Resource
    private HBaseService hbaseService;

    @RequestMapping("/add")
    public String add(@ModelAttribute Userinfos user){
        hbaseService.insert(user);
        return "ok";
    }

    @RequestMapping("/single/{rowkey}")
    public Userinfos select(@PathVariable("rowkey") String rowkey){
        return hbaseService.findByRowkey(rowkey);
    }

    @RequestMapping("/getAll")
    public List<Userinfos> getAll(){
        return hbaseService.findAll();
    }

    @RequestMapping("/findByName/{name}")
    public List<Userinfos> findByName(@PathVariable("name") String name){
        return hbaseService.findUserByPrefixName(name);
    }
}
