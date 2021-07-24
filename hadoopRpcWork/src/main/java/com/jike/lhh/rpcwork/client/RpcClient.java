package com.jike.lhh.rpcwork.client;

import com.jike.lhh.rpcwork.server.StudentInterface;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * RPC客户端
 *
 * @author lianghuahuang
 * @date 2021/7/24
 **/
public class RpcClient {

    public static void main(String[] args){
        try {
           StudentInterface proxy =  RPC.getProxy(StudentInterface.class,1L,new InetSocketAddress("127.0.0.1",6780),new Configuration());
           System.out.println(String.format("输入学号20210000000000，返回：%s",proxy.findName("20210000000000")));
           System.out.println(String.format("输入学号20210123456789，返回：%s",proxy.findName("20210123456789")));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

