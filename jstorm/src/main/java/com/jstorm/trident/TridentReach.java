/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jstorm.trident;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.ReadOnlyState;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.ReadOnlyMapState;
import storm.trident.tuple.TridentTuple;

/**
 * http://qifuguang.me/2015/11/27/Storm%E4%B8%AD%E6%96%87%E6%96%87%E6%A1%A3-Trident%E6%95%99%E7%A8%8B/#comments
 */
public class TridentReach {

    private static Logger LOG = LoggerFactory.getLogger(TridentReach.class);

    public static Map<String, List<String>> TWEETERS_DB = new HashMap<String, List<String>>() {{
        put("foo.com/blog/1", Arrays.asList("sally", "tim"));
        put("foo.com/blog/2", Arrays.asList("sally", "bob", "tim", "george", "nathan"));
        put("engineering.twitter.com/blog/5", Arrays.asList("adam", "david", "sally", "nathan"));
        put("tech.backtype.com/blog/123", Arrays.asList("tim", "mike", "john"));
    }};

    public static Map<String, List<String>> FOLLOWERS_DB = new HashMap<String, List<String>>() {{
        put("sally", Arrays.asList("bob", "tim", "alice", "adam", "jim", "chris", "jai"));
        put("bob", Arrays.asList("sally", "nathan", "jim", "mary", "david", "vivian"));
        put("tim", Arrays.asList("alex"));
        put("nathan", Arrays.asList("sally", "bob", "adam", "harry", "chris", "vivian", "emily", "jordan"));
        put("adam", Arrays.asList("david", "carissa"));
        put("mike", Arrays.asList("john", "bob"));
        put("john", Arrays.asList("alice", "nathan", "jim", "mike", "bob"));
    }};

    public static class StaticSingleKeyMapState extends ReadOnlyState implements ReadOnlyMapState<Object> {
        public static class Factory implements StateFactory {
            Map _map;

            public Factory(Map map) {
                _map = map;
            }

            @Override
            public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
                return new StaticSingleKeyMapState(_map);
            }

        }

        Map _map;

        public StaticSingleKeyMapState(Map map) {
            _map = map;
        }


        @Override
        public List<Object> multiGet(List<List<Object>> keys) {
            List<Object> ret = new ArrayList();
            for (List<Object> key : keys) {
                Object singleKey = key.get(0);
                ret.add(_map.get(singleKey));
            }
            return ret;
        }

    }

    public static class One implements CombinerAggregator<Integer> {
        @Override
        public Integer init(TridentTuple tuple) {
            return 1;
        }

        @Override
        public Integer combine(Integer val1, Integer val2) {
            return 1;
        }

        @Override
        public Integer zero() {
            return 1;
        }
    }

    public static class ExpandList extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            List l = (List) tuple.getValue(0);
            if (l != null) {
                for (Object o : l) {
                    collector.emit(new Values(o));
                }
            }
        }

    }

    public static StormTopology buildTopology(LocalDRPC drpc) {
        TridentTopology topology = new TridentTopology();
        TridentState urlToTweeters = topology.newStaticState(new StaticSingleKeyMapState.Factory(TWEETERS_DB));
        TridentState tweetersToFollowers = topology.newStaticState(new StaticSingleKeyMapState.Factory(FOLLOWERS_DB));


        topology.newDRPCStream("reach", drpc)
                .stateQuery(urlToTweeters, new Fields("args"), new MapGet(), new Fields("tweeters"))
                .each(new Fields("tweeters"), new ExpandList(), new Fields("tweeter"))
                .shuffle()
                .stateQuery(tweetersToFollowers, new Fields("tweeter"), new MapGet(), new Fields("followers"))
                .each(new Fields("followers"), new ExpandList(), new Fields("follower"))
                .groupBy(new Fields("follower"))
                .aggregate(new One(), new Fields("one"))
                .aggregate(new Fields("one"), new Sum(), new Fields("reach"));
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        LocalDRPC drpc = new LocalDRPC();

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("reach", conf, buildTopology(drpc));

        Thread.sleep(2000);

        LOG.info("REACH: " + drpc.execute("reach", "aaa"));
        LOG.info("REACH: " + drpc.execute("reach", "foo.com/blog/1"));
        LOG.info("REACH: " + drpc.execute("reach", "foo.com/blog/2"));
        LOG.info("REACH: " + drpc.execute("reach", "engineering.twitter.com/blog/5"));


        cluster.shutdown();
        drpc.shutdown();
    }
}
