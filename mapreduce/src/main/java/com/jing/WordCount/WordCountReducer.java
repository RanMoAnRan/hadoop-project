package com.jing.WordCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author RanMoAnRan
 * @ClassName: WordCountReducer
 * @projectName hadoop-project
 * @description: reducer类
 * @date 2019/7/5 10:40
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        for (LongWritable value : values) {
            count+=value.get();
        }
        context.write(key,new LongWritable(count));
    }
}
