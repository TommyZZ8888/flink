package com.www.flinkstart.flinkexample.loaddata.socket;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

/**
 * @Description SocketServer
 * @Author 张卫刚
 * @Date Created on 2023/7/14
 */
public class SocketServer {
    public static void main(String[] args) throws IOException {

        try {
            ServerSocket ss = new ServerSocket(9966);
            System.out.println("启动 server ....");
            Socket s = ss.accept();
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()));
            String response = "java,python,c++,";

            //每 2s 发送一次消息
            while (true) {
                Thread.sleep(2000);
                bw.write(response);
                bw.flush();
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
