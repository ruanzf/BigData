package com.hadoop.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by ruan on 2016/5/5.
 */
public class HbaseKeyReducer extends TableReducer<Text, IntWritable, Text> {

    Logger logger = LoggerFactory.getLogger(HbaseKeyReducer.class);
    private Text outputKey = new Text();

    public void reduce(Text key, Iterable<IntWritable> values, Context context
    ) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        logger.info(String.format("HbaseKeyReducer key:{%s}, value:{%s}", key.toString(), sum));

        String rowKey = "sum:" + Bytes.toString(key.copyBytes());
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("rzf"), null, Bytes.toBytes(sum));
        outputKey.set(Bytes.toBytes(rowKey));
        context.write(outputKey, put);
    }
}
