import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * http://spark.apache.org/docs/1.6.2/streaming-programming-guide.html#overview
 * Created by ruanzf on 2016/8/24.
 */
public class KafkaStream {

    /**
     * server: main
     * @param args
     */
    public static void main(String[] args) {
        Map<String, Integer> kafkaTopics = new HashMap<String, Integer>();
        kafkaTopics.put("sicweb", 4);

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("KafkaWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
        JavaPairReceiverInputDStream<String, String> kafkaStream = KafkaUtils.createStream(
                jssc, "192.168.21.37:2181", "kafka_stream", kafkaTopics
        );

        // Split each line into words
        JavaDStream<String> words = kafkaStream.flatMap(new FlatMapFunction<Tuple2<String,String>, String>() {
            public Iterable<String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return Arrays.asList(stringStringTuple2._2.split(" "));
            }
        });

        // Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }
        );

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                }
        );

        // Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();
        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate
    }
}
