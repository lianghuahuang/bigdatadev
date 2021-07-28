package com.jike.lhh.hbase;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * 学员表及其数据操作测试程序
 * @author lianghuahuang
 * @date 2021/7/28
 **/
public class StudentMainProg {
    private static final Logger logger = LoggerFactory.getLogger(StudentMainProg.class);

    public static void main(String[] args){
        Configuration configuration =ConfigurationUtil.getConfiguration();

        Connection connection =getConnection(configuration);

        try {
            StudentDAO studentDAO = new StudentDAO(connection,"lianghuahuang");
            //创建学员表
            logger.info("创建表lianghuahuang:student");
            studentDAO.createTable("name", "info", "score");
            //插入数据
            List<StudentDTO> list = Lists.newArrayList();
            logger.info("初始化学员信息：");
            initialStudents(list);
            for (StudentDTO dto : list) {
                studentDAO.insert(dto);
                logger.info(dto.toString());
            }

            //查询rowKey=‘stu-5’的学员信息
            StudentDTO result = studentDAO.get("stu-5");
            logger.info("查询rowKey=‘stu-5’的学员信息结果:");
            logger.info(result.toString());
            //查询所有学员信息
            List<StudentDTO> resultList = studentDAO.list(null, null);
            logger.info("查询所有学员信息结果:");
            resultList.forEach(studentDTO -> {
                logger.info(studentDTO.toString());
            });


            //删除学员数据
            logger.info("删除rowkey=stu-5学员记录");
            studentDAO.delete("stu-5");

            //删除表
            logger.info("删除表lianghuahuang:student");
            studentDAO.dropTable();
        }finally {
            if(connection!=null&&!connection.isClosed()) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    /**
     * 初始化学员数据
     * @param list
     */
    private static void initialStudents(List<StudentDTO> list) {
        StudentDTO tom =new StudentDTO("stu-1","Tom","20210000000001","1","75","82");
        list.add(tom);
        StudentDTO jerry =new StudentDTO("stu-2","Jerry","20210000000002","1","85","67");
        list.add(jerry);
        StudentDTO jack =new StudentDTO("stu-3","Jack","20210000000003","2","80","80");
        list.add(jack);
        StudentDTO rose =new StudentDTO("stu-4","Rose","20210000000004","2","60","61");
        list.add(rose);
        StudentDTO lianghuahuang =new StudentDTO("stu-5","梁华璜","G20210735010446","2","80","85");
        list.add(lianghuahuang);
    }

    public static Connection getConnection(Configuration configuration) {
        try {
            //检查配置
            HBaseAdmin.available(configuration);
            return ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

