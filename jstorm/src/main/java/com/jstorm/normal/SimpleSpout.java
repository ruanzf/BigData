package com.jstorm.normal;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.batch.BatchId;
import com.alibaba.jstorm.batch.IBatchSpout;
import java.util.Map;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ruan on 2016/6/16.
 */
public class SimpleSpout implements IBatchSpout {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleSpout.class);
    private Random rand;
    private int batchSize = 100;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        rand = new Random();
        rand.setSeed(System.currentTimeMillis());
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        BatchId batchId = (BatchId) input.getValue(0);
        String id = String.format("the batchId is:%s", batchId.getId());
        LOG.info(id);
        for (int i = 0; i < batchSize; i++) {
            long value = rand.nextInt(10);
            collector.emit(new Values(batchId, value));
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("BatchId", "Value"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public byte[] commit(BatchId id) throws FailedException {
        LOG.info("Receive BatchId " + id);
        return null;
    }

    @Override
    public void revert(BatchId id, byte[] commitResult) {
        LOG.info("Receive BatchId " + id);
    }

}