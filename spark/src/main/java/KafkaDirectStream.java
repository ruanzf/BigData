import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * http://spark.apache.org/docs/1.6.2/streaming-programming-guide.html#overview
 * Created by ruanzf on 2016/8/24.
 */
public class KafkaDirectStream {

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
        // Split each line into words
        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();
        JavaDStream<String> javaDStream = kafkaStream.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
            public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                offsetRanges.set(offsets);
                for (OffsetRange o : offsets) {
                    System.out.println("#########################################" +
                            o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset()
                    );
                }
                return rdd;
            }
        });

        JavaPairDStream<String, Integer> wordCounts = javaDStream.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterable<String> call(String x) {
                        System.out.println(x + "  %%%%%%%%%%%%%%%%%%");
                        return Arrays.asList(x.split(" "));
                    }
                }
        ).mapToPair(        // Count each word in each batch
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }
        ).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                }
        );

        //  Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();

        wordCounts.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            public void call(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) throws Exception {
                OffsetRange[] offsets = offsetRanges.get();
                if (null == offsetRanges) {
                    System.out.println("***************offsetRanges is null");
                }else {
                    for (OffsetRange o : offsets) {
                        System.out.println("***********" +
                                o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset()
                        );
                    }
                }
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
