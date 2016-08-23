import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by ruanzf on 2016/8/8.
 */

public class WordCount {

    public static void main(String[] args) {
        String logFile = "/data/tools/spark-1.6.2/README.md"; //该文本一定要有
        SparkConf conf = new SparkConf().setAppName("Word Count Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        //用空格分割出一个个单词
        JavaRDD<String> words = logData.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) { return Arrays.asList(s.split(" ")); }
        });
        //为每个单词计数为1
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) { return new Tuple2<String, Integer>(s, 1); }
        });
        //按单词名字，合并单词数
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) { return a + b; }
        });

        //打印结果
        counts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println("word: " + stringIntegerTuple2._1 + ", count: " + stringIntegerTuple2._2);
            }
        });
    }
}