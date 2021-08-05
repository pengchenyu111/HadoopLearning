package com.pcy.mapreduce.writable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 统计每一个手机号耗费的总上行流量、总下行流量、总流量
 * <p>
 * 输入：
 * 7 13560436666 120.196.100.99 1116 954 200
 * id 手机号码 网络 ip 上行流量 下行流量 网络状态码
 * <p>
 * 输出：
 * 13560436666 1116 954 2070
 * 手机号码 上行流量 下行流量 总流量
 *
 * @author PengChenyu
 * @since 2021-08-05 14:41:04
 */
public class FlowDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2 设置jar包
        job.setJarByClass(FlowDriver.class);

        //3 设置关联mapper和reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4 设置mapper输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5 设置程序最终输出的 KV 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //6 设置程序的输入输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\MyOwnCodes\\IJIDEAJAVA\\HadoopLearning\\input\\inputflow"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\MyOwnCodes\\IJIDEAJAVA\\HadoopLearning\\output\\outputflow"));

        //7 提交 Job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
