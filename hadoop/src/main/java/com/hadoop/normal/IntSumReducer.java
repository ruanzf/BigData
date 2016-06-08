package com.hadoop.normal;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by ruan on 2016/5/5.
 */
public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    Logger logger = LoggerFactory.getLogger(IntSumReducer.class);
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        String log = String.format("IntSumReducer key:{%s}, value:{%s}", key.toString(), sum);
        logger.info(log);
        System.err.println(log);
        context.write(key, result);
    }
}
