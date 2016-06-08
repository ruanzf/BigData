package com.kafka.normal;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Created by ruan on 2016/6/6.
 */
public class MyConsumer {

    public static final String zkPath = "/consumers/%s/offsets/%s/%s/";
    public static final CuratorFramework client;

    static {
        client = CuratorFrameworkFactory.builder().build();
    }

    public static void main(String[] args) throws InterruptedException {
        subscribe();
    }

    private static void seek() throws InterruptedException {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(initConf());
        consumer.assign(Arrays.asList(new TopicPartition("test", 0)));
        consumer.seek(new TopicPartition("test", 0), 80);
        ConsumerRecords<String, String> records = consumer.poll(1000);
        for (ConsumerRecord<String, String> record : records)
            System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());

        //生效
//        consumer.commitSync();

        //但是不生效  不知道为什么
        commitAsync(consumer);

    }

    private static void commitAsync(KafkaConsumer<String, String> consumer) {
        consumer.commitAsync(new OffsetCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                if (null != exception) {
                    exception.printStackTrace();
                }else {
                    offsets.forEach((topicPartition, offsetAndMetadata) -> {
                        System.out.println(String.format("this is call back, topic %s, partition %s offset %s",
                                topicPartition.topic(), topicPartition.partition(), offsetAndMetadata.offset()));
                    });
                }
            }
        });
    }

    private static void keepOffsetToZk(String clientName, String topic, String partition) {
        String path = String.format(zkPath, clientName, topic, partition);

    }

    private static void subscribe() throws InterruptedException {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(initConf());
        consumer.subscribe(Arrays.asList("test"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());

            Thread.sleep(1000);

            if(records.count() > 0) {
                //生效
                commitAsync(consumer);
            }
        }


    }

    /**
     * http://kafka.apache.org/documentation.html#producerconfigs
     * @return
     */
    private static Properties initConf() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.118:9092");
        props.put("group.id", "test");
        props.put("client.id", "ruanzf-test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

}
