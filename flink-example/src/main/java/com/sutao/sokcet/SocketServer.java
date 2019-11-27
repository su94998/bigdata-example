package com.sutao.sokcet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by sutao on 2019/11/13.
 */
public class SocketServer {

    private final static Logger LOGGER = LoggerFactory.getLogger(SocketServer.class);

    //搭建服务器端
    public static void main(String[] args) throws IOException {
        ServerSocket serverSocket = null;
        BufferedReader reader = null;
        BufferedWriter writer = null;
        try {
            serverSocket = new ServerSocket(6679);  //端口号
            LOGGER.info("服务端服务启动监听：");
            //通过死循环开启长连接，开启线程去处理消息
            while (true) {
                Socket socket = serverSocket.accept();
                try {
                    reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));//读取客户端消息
                    writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));//向客户端写消息
                    String lineString = "";

                    while (!(lineString = reader.readLine()).equals("bye")) {
                        LOGGER.info("收到来自客户端的发送的消息是：" + lineString);
                        writer.write("服务器返回：" + lineString + "\n");
                        writer.flush();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (reader != null) {
                            reader.close();
                        }
                        if (writer != null) {
                            writer.close();
                        }
                        if (socket != null) {
                            socket.close();
                        }
                    } catch (Exception e2) {
                        e2.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (serverSocket != null) {
                    serverSocket.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }
}


