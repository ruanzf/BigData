package com.hadoop.normal;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Created by ruan on 2016/5/5.
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    Logger logger = LoggerFactory.getLogger(TokenizerMapper.class);
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    private Set<String> patternsToSkip = new HashSet<String>();

    @Override
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String line = value.toString().toLowerCase();
        for (String pattern : patternsToSkip) {
            line = line.replaceAll(pattern, "");
        }
        StringTokenizer itr = new StringTokenizer(line);
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken();
            String log = String.format("TokenizerMapper key:{%s}", token);
            logger.info(log);
            word.set(token);
            context.write(word, one);
        }
    }
}
