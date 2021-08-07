package com.jike.lhh.hive.fileformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * hive自定义文本输出格式进行数据加密操作
 *
 * @author lianghuahuang
 * @date 2021/8/6
 **/
public class GeekTextOuputFormat<K extends WritableComparable, V extends Writable>
        extends HiveIgnoreKeyTextOutputFormat<K, V> {

    /**
     * GeekRecordWriter.
     *
     */
    public static class GeekRecordWriter implements FileSinkOperator.RecordWriter {

        RecordWriter writer;
        Text text;

        public GeekRecordWriter(RecordWriter writer) {
            this.writer = writer;
            text = new Text();
        }

        @Override
        public void write(Writable w) throws IOException {
            if (w instanceof Text) {
                String input = ((Text) w).toString();
                //处理加密操作，加密规则：文件输出时每随机2到256个单词，就插入一个gee...k，字母e的个数等于前面出现的非gee...k单词的个数。
                String[] array = input.split("(?=\\s)");
                int max = array.length>256?256:array.length;
                int min = 2;
                int count = 0;
                while(min<array.length) {
                    int ran = (int) (Math.random() * (max - min) + min);
                    StringBuffer sb = new StringBuffer(array[ran]).append(" g");
                    for (int j = 0; j < (count==0?ran+1:(ran-min+2)); j++) {
                        sb.append("e");
                    }
                    sb.append("k");
                    array[ran] = sb.toString();
                    min = ran+2;
                    count++;
                }

                StringBuilder builder = new StringBuilder();
                for (String value: array) {
                    builder.append(value);
                }
                text.set(builder.toString());
                writer.write(text);
            }
        }

        @Override
        public void close(boolean abort) throws IOException {
            writer.close(abort);
        }

    }

    @Override
    public RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
                                            Class<? extends Writable> valueClass, boolean isCompressed,
                                            Properties tableProperties, Progressable progress) throws IOException {

        GeekRecordWriter writer = new GeekRecordWriter(super
                .getHiveRecordWriter(jc, finalOutPath, Text.class,
                        isCompressed, tableProperties, progress));
        return writer;
    }
}

