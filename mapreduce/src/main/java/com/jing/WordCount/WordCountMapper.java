package com.jing.WordCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author RanMoAnRan
 * @ClassName: WordCountMapper
 * @projectName hadoop-project
 * @description: mapper类
 * @date 2019/7/5 10:36
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text,LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split(",");
        for (String s1 : split) {
            context.write(new Text(s1),new LongWritable(1));
        }
    }
}
