package com.hadoop.hbase;

import com.hadoop.normal.TokenizerMapper;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by ruan on 2016/5/30.
 */
public class HbaseKeyMapper extends TableMapper<Text, IntWritable> {

    Logger logger = LoggerFactory.getLogger(TokenizerMapper.class);
    private IntWritable outputValue = new IntWritable(0);
    private Text outputKey = new Text();

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String rowKey = Bytes.toString(key.copyBytes());
        int sumValue = 0;
        for(Cell cell : value.listCells()) {
            sumValue += Bytes.toInt(CellUtil.cloneValue(cell));
        }

        logger.info(String.format("HbaseKeyMapper key:%s, value:%s", rowKey, sumValue));
        outputKey.set(rowKey.substring(0, 3));
        outputValue.set(sumValue);
        context.write(outputKey, outputValue);
    }
}
