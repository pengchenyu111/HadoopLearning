package com.pcy.mapreduce.combineTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 小文件的切片处理
 *
 * @author PengChenyu
 * @since 2021-08-04 17:06:29
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 设置jar包路径
        job.setJarByClass(WordCountDriver.class);

        // 3 关联mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4 设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出的kV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 显式设置CombineTextInputFormat，否则会默认使用TextInputFormat
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 设置虚拟存储切片的最大值,20m => 合并小文件为一个切片
        CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);

        // 6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\MyOwnCodes\\IJIDEAJAVA\\HadoopLearning\\input\\inputcombinetextinputformat"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\MyOwnCodes\\IJIDEAJAVA\\HadoopLearning\\output\\outputCombine"));

        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
