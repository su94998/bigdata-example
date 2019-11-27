package com.sutao.sokcet;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;

/**
 * Created by sutao on 2019/11/13.
 */

public class SocketClient {
    public static void main(String[] args) {

        Socket socket=null;
        BufferedReader reader=null;
        BufferedWriter writer=null;
        try {
            socket=new Socket("127.0.0.1", 10000);
            reader = new BufferedReader(new InputStreamReader(System.in));
            writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            String lineString="";
            while(!(lineString=reader.readLine()).equals("exit")){
                writer.write(lineString+"\n");
                writer.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader!=null) {
                    reader.close();
                }
                if (writer!=null) {
                    writer.close();
                }
                if (socket!=null) {
                    socket.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }

    }


}

