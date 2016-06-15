package com.kafka.normal;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Created by ruan on 2016/6/6.
 */
public class MyConsumer {

    public static final String zkPath = "/consumers/ruanzf-test/offsets/test/0";
    public static final CuratorFramework client;

    static {
        client = CuratorFrameworkFactory.newClient("192.168.1.118:2181", new RetryNTimes(3, 1000));
        client.start();
    }

    public static void main(String[] args) throws InterruptedException {
        subscribe();
        seek();
    }

    private static void seek() throws InterruptedException {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(initConf());
        consumer.assign(Arrays.asList(new TopicPartition("test", 0)));

        while (true) {
            long offset = getOffsetToZk();
            consumer.seek(new TopicPartition("test", 0), offset);
            ConsumerRecords<String, String> records = consumer.poll(1000);

            if (records.count() <= 0)
                break;

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                offset = record.offset();
            }

            setOffsetToZk(offset);
            consumer.commitSync();
        }

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

    private static long getOffsetToZk() {
        try {
            if(null == client.checkExists().forPath(zkPath)) {
                client.create().creatingParentsIfNeeded().forPath(zkPath, "0".getBytes());
            }
            return Long.valueOf(new String(client.getData().forPath(zkPath))) + 1;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    private static void setOffsetToZk(long offset) {
        try {
            client.setData().forPath(zkPath, Long.toString(offset).getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void subscribe() throws InterruptedException {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(initConf());
        consumer.subscribe(Arrays.asList("test"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());

            if(records.count() > 0)
                commitAsync(consumer);

            Thread.sleep(1000);
        }


    }

    /**
     * http://kafka.apache.org/090/documentation.html#newconsumerconfigs
     * @return
     */
    private static Properties initConf() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.118:9092");
        props.put("group.id", "test");
        props.put("client.id", "ruanzf-test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");

        /**
         * 用这个配置来控制一次取出的数据量
         * seek() 遍可以不断的更新zk中的offset取出数据
         */
        props.put("max.partition.fetch.bytes", "1024");

        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

}
