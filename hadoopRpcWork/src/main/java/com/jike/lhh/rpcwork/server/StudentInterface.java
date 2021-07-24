package com.jike.lhh.rpcwork.server;


import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * 学生业务接口
 * @author lianghuahuang
 * @date 2021/7/24
 **/
public interface StudentInterface extends VersionedProtocol {

    long versionID = 1L;

    /**
     * 输入真实学号，返回真实姓名
     * @param studentId 学号
     * @return 真实姓名
     */
    String findName(String studentId);
}

