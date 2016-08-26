import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.util.*;

/**
 * http://spark.apache.org/docs/1.6.2/streaming-programming-guide.html#overview
 * Created by ruanzf on 2016/8/24.
 */
public class KafkaRedisDirectStream {

    /**
     * server: main
     * @param args
     */
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("KafkaWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
        JavaInputDStream<String> kafkaStream = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                String.class,
                kafkaParams(),
                fromOffsets(),
                new Function<MessageAndMetadata<String, String>, String>() {
                    public String call(MessageAndMetadata<String, String> v1) throws Exception {
                        return v1.message();
                    }
                }
                );

        kafkaStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                stringJavaRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
                    public void call(Iterator<String> stringIterator) throws Exception {
                        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
                        jedisClusterNodes.add(new HostAndPort("192.168.21.", 63));
                        JedisCluster jedisCluster = new JedisCluster(jedisClusterNodes);
                        long time = (System.currentTimeMillis() / 60000) * 60000;
                        System.out.println(time + " =======================");
                        jedisCluster.hincrBy(time + ":key", "count", 1l);
                    }
                });
            }
        });

        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate
    }

    private static Map<String, String> kafkaParams() {
        Map<String, String> params = new HashMap<String, String>();
        params.put("metadata.broker.list", "192.168.21.");
        params.put("group.id", "spark-analyzer");
        params.put("fetch.message.max.bytes", "1048576");
        return params;
    }

    private static Map<TopicAndPartition, Long> fromOffsets() {
        Map<TopicAndPartition, Long> partitionMap = new HashMap<TopicAndPartition, Long>();
        partitionMap.put(new TopicAndPartition("test", 0), 1l);
        return partitionMap;
    }
}
