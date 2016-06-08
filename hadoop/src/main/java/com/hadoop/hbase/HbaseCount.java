package com.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;


/**
 * Created by ruan on 2016/5/5.
 */
public class HbaseCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "hbase value sum");
        job.setJarByClass(HbaseCount.class);
        Scan scan = initScan();

        TableMapReduceUtil.initTableMapperJob("test", scan, HbaseKeyMapper.class, Text.class, IntWritable.class, job);
        TableMapReduceUtil.initTableReducerJob("test", HbaseKeyReducer.class, job);

        job.setCombinerClass(HbaseKeyCombiner.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static Scan initScan() {
        Scan scan = new Scan();
        scan.setStartRow("a:q:00".getBytes());
        scan.setStopRow("e:z:20".getBytes());
        return scan;
    }
}
