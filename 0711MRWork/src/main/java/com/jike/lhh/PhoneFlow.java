package com.jike.lhh;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PhoneFlow {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, FlowBean>{

    private Text phone = new Text();

    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      //先获取行记录
      String[] rows = value.toString().split("\\n");
      for (String row : rows) {
        String[] result = row.split("\\s+");
        //一行第二列为手机号、最后一列为响应码、倒数第二、三列分别为下行上行流量
        phone.set(result[1]);
        FlowBean flowBean = new FlowBean();
        if(StringUtils.isNumeric(result[result.length-3])){
          flowBean.setUpFlow(Long.parseLong(result[result.length-3]));
        }
        if(StringUtils.isNumeric(result[result.length-2])){
          flowBean.setDownFlow(Long.parseLong(result[result.length-2]));
        }
        context.write(phone, flowBean);
      }
    }
  }

  public static class LongSumReducer
       extends Reducer<Text,FlowBean,Text,FlowBean> {
    private FlowBean result = new FlowBean();

    @Override
    public void reduce(Text key, Iterable<FlowBean> values,
                       Context context
                       ) throws IOException, InterruptedException {
      long upFlow = 0;
      long downFlow = 0;
      for (FlowBean val : values) {
        upFlow+=val.getUpFlow();
        downFlow+=val.getDownFlow();
      }
      result.set(upFlow,downFlow);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "PhoneFlow");
    job.setJarByClass(PhoneFlow.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(LongSumReducer.class);
    job.setReducerClass(LongSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FlowBean.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}