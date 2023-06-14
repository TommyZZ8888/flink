package com.www.flinkstart.flinkexample.closure;

/**
 * @Description Programmer
 * @Author 张卫刚
 * @Date Created on 2023/6/12
 */
public class Programmer {

    private String name;

    public Programmer() {
        super();
    }

    public Programmer(String name) {
        super();
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void work() {
        System.out.println(name + "正在编程");
    }
}
