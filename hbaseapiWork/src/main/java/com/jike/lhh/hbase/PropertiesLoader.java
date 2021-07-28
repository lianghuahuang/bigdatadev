package com.jike.lhh.hbase;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * hbase属性文件加载
 * @author lianghuahuang
 * @date 2021/7/27
 **/
public class PropertiesLoader {
   private static Properties properties = new Properties();

   static{
       InputStream in = PropertiesLoader.class.getClassLoader().getResourceAsStream("hbase.properties");
       try {
           properties.load(in);
       } catch (IOException e) {
           e.printStackTrace();
       }
   }

   public static String get(String key){
        return properties.getProperty(key);
   }

}

