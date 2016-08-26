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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * http://spark.apache.org/docs/1.6.2/streaming-programming-guide.html#overview
 * Created by ruanzf on 2016/8/24.
 */
public class KafkaEsDirectStream {

    /**
     * server: main
     * @param args
     */
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("KafkaWordCount");
        conf.set("es.nodes", "192.168.21.");
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

        kafkaStream.filter(new Function<String, Boolean>() {
            public Boolean call(String v1) throws Exception {
                if (v1.startsWith("linesum")) {
                    return true;
                }else {
                    return false;
                }
            }
        }).map(new Function<String, String>() {
            public String call(String v1) throws Exception {
                return "{\"value\":\""+v1+"\"}";
            }
        }).foreachRDD(new VoidFunction<JavaRDD<String>>() {

            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                JavaEsSpark.saveJsonToEs(stringJavaRDD, "test/spark");
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
