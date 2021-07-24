package com.jike.lhh.rpcwork.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * HadoopRpcServer端
 *
 * @author lianghuahuang
 * @date 2021/7/24
 **/
public class HadoopRpcServer {

    public static void main(String[] args){
        RPC.Builder builder = new RPC.Builder(new Configuration());
        builder.setBindAddress("127.0.0.1");
        builder.setPort(6780);
        builder.setProtocol(StudentInterface.class);
        builder.setInstance(new StudentInterfaceImpl());

        try {
            RPC.Server server = builder.build();
            server.start();
            System.out.print("HadoopRpcServer端已启动，等待调用");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

