package com.pcy.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper   注意导包，四个泛型参数描述如下
 * KEYIN   map阶段输入的key的类型：LongWritable
 * VALUEIN  map阶段输入value类型：Text
 * KEYOUT   map阶段输出的Key类型：Text
 * VALUEOUT map阶段输出的value类型：IntWritable
 *
 * @author PengChenyu
 * @since 2021-08-04 17:06:09
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);

    /**
     * 注意不要在map种创建对象，因为会执行很多次
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行
        // atguigu atguigu
        String line = value.toString();
        // 2 切割
        // atguigu
        // atguigu
        String[] words = line.split(" ");
        // 3 循环写出
        for (String word : words) {
            // 封装outk
            outK.set(word);
            // 写出
            context.write(outK, outV);
        }

    }
}
