package com.jike.lhh.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * HBase配置加载
 * @author lianghuahuang
 * @date 2021/7/28
 **/
public class ConfigurationUtil {
    private static final Logger logger = LoggerFactory.getLogger(ConfigurationUtil.class);
    /**
     * 获取配置
     *
     * @return
     */
    public static Configuration getConfiguration() {
        String clientPort = PropertiesLoader.get("hbase.zookeeper.property.clientPort");
        String quorum = PropertiesLoader.get("hbase.zookeeper.quorum");
        logger.info("connect to zookeeper {}:{}", quorum, clientPort);
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.property.clientPort", clientPort);
        config.set("hbase.zookeeper.quorum", quorum);
        return config;
    }
}

