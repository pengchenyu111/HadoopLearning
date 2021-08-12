package com.pcy.mapreduce.partitioner;

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
 * 手机号 136、137、138、139 开头都分别放到一个独立的 4 个文件中，其他开头的放到一个文件中
 *
 * （1）如果ReduceTask的数量> getPartition的结果数，则会多产生几个空的输出文件part-r-000xx；
 * （2）如果1<ReduceTask的数量<getPartition的结果数，则有一部分分区数据无处安放，会Exception；
 * （3）如 果ReduceTask的数量=1，则不管MapTask端输出多少个分区文件，最终结果都交给这一个ReduceTask，最终也就只会产生一个结果文件 part-r-00000；
 * （4）分区号必须从零开始，逐一累加。
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

        // 设置分区器和ReduceTask数量
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);

        //6 设置程序的输入输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\MyOwnCodes\\IJIDEAJAVA\\HadoopLearning\\input\\inputflow"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\MyOwnCodes\\IJIDEAJAVA\\HadoopLearning\\output\\outputflowPartitioner"));

        //7 提交 Job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
