package com.bigdata.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class MyConsumer {
    public static void main(String[] args) {

        Properties props = new Properties();

        // Kafka集群
        props.put("bootstrap.servers", "hadoop101:9092,hadoop102:9092,hadoop103:9092");

        // Consumer组id
        props.put("group.id", "jone");

        // Consumer是否自动提交Offset
        props.put("enable.auto.commit", "true");

        // Consumer提交Offset的间隔
        props.put("auto.commit.interval.ms", 1000);

        // kv反序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 构建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // Consumer订阅哪些Topic
        consumer.subscribe(Arrays.asList("first", "hello"));
        while (true) {
            // Consumer主动拉取数据，用poll方法
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("topic:" + record.topic() + "\t" + "partition:" + record.partition() + "\t" + "value:" + record.value());
            }
        }
    }
}
