package com.jike.lhh.hive.fileformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * hive 自定义文本输入格式进行数据解密操作
 *
 * @author lianghuahuang
 * @date 2021/8/6
 **/
public class GeekTextInputFormat implements
        InputFormat<LongWritable, Text>, JobConfigurable {

    TextInputFormat format;
    JobConf job;


    public static class GeekLineRecordReader implements
            RecordReader<LongWritable, Text> {

        LineRecordReader reader;
        Text text;
        //解密规则正则匹配：文件中出现任何的geek，geeeek，geeeeeeeeeeek等单词时，进行过滤，即删除该单词。gek需要保留。字母中连续的“e”最大长度为256个。
        static  final String DEC_PATTERN = "\\s{1}ge{2,256}k";
        static final Pattern pattern = Pattern.compile(DEC_PATTERN);

        public GeekLineRecordReader(LineRecordReader reader) {
            this.reader = reader;
            text = reader.createValue();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        @Override
        public LongWritable createKey() {
            return reader.createKey();
        }

        @Override
        public Text createValue() {
            return new Text();
        }

        @Override
        public long getPos() throws IOException {
            return reader.getPos();
        }

        @Override
        public float getProgress() throws IOException {
            return reader.getProgress();
        }

        @Override
        public boolean next(LongWritable key, Text value) throws IOException {
            while (reader.next(key, text)) {
                Matcher matcher = pattern.matcher(text.toString());
                //正则匹配后替换为空格，因为英文单词以空格来分割
                String result = matcher.replaceAll(" ");
                value.set(result);
                return true;
            }
            // no more data
            return false;
        }

    }


    public GeekTextInputFormat(){
        format = new TextInputFormat();
    }

    @Override
    public void configure(JobConf job) {
        this.job = job;
        format.configure(job);
    }

    @Override
    public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
        return format.getSplits(job, numSplits);
    }

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        reporter.setStatus(inputSplit.toString());
        GeekLineRecordReader reader = new GeekLineRecordReader(
                new LineRecordReader(job, (FileSplit) inputSplit));
        return reader;
    }
}

