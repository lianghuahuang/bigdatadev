package com.jike.lhh.rpcwork.server;

import com.google.common.collect.Maps;
import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;
import java.util.Map;

/**
 * @author lianghuahuang
 * @date 2021/7/24
 **/
public class StudentInterfaceImpl implements StudentInterface{
    private static final Map<String,String> studentMap = Maps.newHashMap();

    static{
        studentMap.put("20210123456789","心心");
        studentMap.put("20211123456789","金老师");
    }
    /**
     * 输入真实学号，返回真实姓名
     *
     * @param studentId 学号
     * @return 真实姓名
     */
    @Override
    public String findName(String studentId) {
        System.out.println(String.format("接收到客户端调用，输入学号：%s",studentId));
        return studentMap.get(studentId);
    }

    /**
     * Return protocol version corresponding to protocol interface.
     *
     * @param protocol      The classname of the protocol interface
     * @param clientVersion The version of the protocol that the client speaks
     * @return the version that the server will speak
     * @throws IOException if any IO error occurs
     */
    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return versionID;
    }

    /**
     * Return protocol version corresponding to protocol interface.
     *
     * @param protocol          The classname of the protocol interface
     * @param clientVersion     The version of the protocol that the client speaks
     * @param clientMethodsHash the hashcode of client protocol methods
     * @return the server protocol signature containing its version and
     * a list of its supported methods
     */
    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
        return new ProtocolSignature(versionID,null);
    }
}

