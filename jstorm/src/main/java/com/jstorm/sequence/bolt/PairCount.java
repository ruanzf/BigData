/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jstorm.sequence.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.jstorm.TpsCounter;
import com.jstorm.sequence.bean.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PairCount implements IBasicBolt {
    private static final long serialVersionUID = 7346295981904929419L;

    public static final Logger LOG = LoggerFactory.getLogger(PairCount.class);

    private AtomicLong sum = new AtomicLong(0);
    private String componentId;
    private TpsCounter tpsCounter;

    public void prepare(Map conf, TopologyContext context) {
        tpsCounter = new TpsCounter(context.getThisComponentId() + ":" + context.getThisTaskId());
        componentId = context.getThisComponentId();
        LOG.info("Successfully do parepare " + context.getThisComponentId());
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        tpsCounter.count();

        Long tupleId = tuple.getLong(0);
        Pair pair = (Pair) tuple.getValue(1);
        LOG.info("PairCount tupleId: " + tupleId);

        sum.addAndGet(pair.getValue());

        collector.emit(new Values(tupleId, pair));
    }

    public void cleanup() {
        tpsCounter.cleanup();
        LOG.info(componentId + " PairCount Total receive value:" + sum);
        LOG.info("PairCount cleanup");
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ID", "PAIR"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}