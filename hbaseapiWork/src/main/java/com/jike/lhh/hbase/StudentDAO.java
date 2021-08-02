package com.jike.lhh.hbase;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 学员表及数据访问操作
 * @author lianghuahuang
 * @date 2021/7/27
 **/
public class StudentDAO {

    private static final Logger logger = LoggerFactory.getLogger(StudentDAO.class);

    private Connection connection;

    private TableName tableName;

    private String namespace;

    public StudentDAO(Connection connection,String namespace){
        this.connection = connection;
        this.namespace =namespace;
        this.tableName = TableName.valueOf(namespace+":student");
    }

    /**
     * 创建表
     * @param columnFamilies
     * @throws IOException
     */
    public void  createTable(String... columnFamilies)  {
        //创建表

        Admin admin = null;
        try {
            admin = connection.getAdmin();
            if (admin.tableExists(tableName)) {
                logger.warn("table:{} exists!", tableName.getName());
            } else {
                TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
                for (String columnFamily : columnFamilies) {
                    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamily));
                }
                admin.createTable(builder.build());
                logger.info("create table:{} success!", tableName.getName());
            }
        } catch (IOException ie){
            logger.error(ie.getLocalizedMessage());
        }finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    logger.error(e.getLocalizedMessage());
                }
            }
        }
    }


    /**
     * 删除表
     * @throws IOException
     */
    public void dropTable() {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                logger.info("drop table:{} success!", tableName.getName());
            }
        }catch (IOException ie){
              logger.error(ie.getLocalizedMessage());
        }finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    logger.error(e.getLocalizedMessage());
                }
            }
        }
    }

    public void insert( StudentDTO studentDTO){
        Table table = null;
        try {
            table = connection.getTable(tableName);
            Put put = new Put(Bytes.toBytes(studentDTO.getRowKey()));
            put.addColumn(Bytes.toBytes("name"),null,Bytes.toBytes(studentDTO.getName()));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes(studentDTO.getInfo().getStudentId()));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes(studentDTO.getInfo().getClazz()));
            put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes(studentDTO.getScore().getUnderstanding()));
            put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes(studentDTO.getScore().getProgramming()));
            table.put(put);
        }catch (IOException ie){
            logger.error(ie.getLocalizedMessage());
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    logger.error(e.getLocalizedMessage());
                }
            }
        }
    }

    public void delete(String rowKey){
        Table table = null;
        try {
            table = connection.getTable(tableName);
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        }catch (IOException ie){
            logger.error(ie.getLocalizedMessage());
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    logger.error(e.getLocalizedMessage());
                }
            }
        }
    }


    public StudentDTO get(String rowKey) {
        Table table = null;
        try {
            table = connection.getTable(tableName);
            Get get = new Get(Bytes.toBytes(rowKey));

            Result result = table.get(get);
            List<Cell> cells = result.listCells();

            if (CollectionUtils.isEmpty(cells)) {
                return null;
            }
            StudentDTO studentDTO = new StudentDTO();
            studentDTO.setRowKey(Bytes.toString(result.getRow()));
            for (Cell cell : cells) {
                cellToStudentDTO(studentDTO, cell);

            }
            return studentDTO;
        }catch (IOException ie){
            logger.error(ie.getMessage());
            throw new RuntimeException("get error,rowKey="+ rowKey);
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    logger.error(e.getMessage());
                }
            }
        }
    }

    private void cellToStudentDTO(StudentDTO studentDTO, Cell cell) throws UnsupportedEncodingException {
        String family  = new String(CellUtil.cloneFamily(cell));
        String qualifier = new String(CellUtil.cloneQualifier(cell));
        String value = new String(CellUtil.cloneValue(cell), "UTF-8");
        if(family.equals("name")){
            studentDTO.setName(value);
        }else if(family.equals("info")){
            if(qualifier.equals("student_id")){
                studentDTO.getInfo().setStudentId(value);
            }else if(qualifier.equals("class")){
                studentDTO.getInfo().setClazz(value);
            }
        }else if(family.equals("score")){
            if(qualifier.equals("understanding")){
                studentDTO.getScore().setUnderstanding(value);
            }else if(qualifier.equals("programming")){
                studentDTO.getScore().setProgramming(value);
            }
        }
    }

    public List<StudentDTO> list(String rowkeyStart, String rowkeyEnd) {
        Table table = null;
        try {
            table = connection.getTable(tableName);
            ResultScanner rs = null;
            try {
                Scan scan = new Scan();
                if (!StringUtils.isEmpty(rowkeyStart)) {
                    scan.withStartRow(Bytes.toBytes(rowkeyStart));
                }
                if (!StringUtils.isEmpty(rowkeyEnd)) {
                    scan.withStopRow(Bytes.toBytes(rowkeyEnd));
                }
                rs = table.getScanner(scan);

                List<StudentDTO> dataList = new ArrayList<>();
                for (Result r : rs) {
                    StudentDTO studentDTO = new StudentDTO();
                    studentDTO.setRowKey(Bytes.toString(r.getRow()));
                    for (Cell cell : r.listCells()) {
                        cellToStudentDTO(studentDTO, cell);
                    }
                    dataList.add(studentDTO);
                }
                return dataList;
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
        }catch (IOException ie){
            logger.error(ie.getMessage());
            throw new RuntimeException("list error,rowkeyStart="+ rowkeyStart+",rowkeyEnd="+rowkeyEnd);
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    logger.error(e.getMessage());
                }
            }
        }
    }

}

