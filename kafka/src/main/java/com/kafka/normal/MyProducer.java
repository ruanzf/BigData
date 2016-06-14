package com.kafka.normal;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * Created by ruan on 2016/6/3.
 */
public class MyProducer {

    public static void main(String[] args) {
        Properties props = initConf();
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for(int i = 0; i < 500; i++)
            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), Integer.toString(i)),
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            System.out.println("this is call back");
                            if(exception != null)
                                exception.printStackTrace();

                            System.out.printf("topic: %s partition: %s, offset %s \n", metadata.topic(), metadata.partition(), metadata.offset());
                        }
                    });
        producer.close();
    }

    /**
     * http://kafka.apache.org/documentation.html#producerconfigs
     * @return
     */
    private static Properties initConf() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.118:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}
