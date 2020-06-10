package com.bigdata.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.util.Properties;

public class MyProducer {
    public static void main(String[] args) {

        Properties props = new Properties();

        // Kafka集群地址
        props.put("bootstrap.servers", "hadoop101:9092,hadoop102:9092,hadoop103:9092");

        // ACK应答机制，取值有0,1,all,效率和安全性
        props.put("acks", "all");

        // 生产者向kafka发送消息出现错误时的重试次数
        props.put("retries", 0);

        // 批处理大小和发送延迟，每个Batch要存放16k数据或者1ms后才可以发到Broker上去
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);

        // 内存缓冲区大小，通过KafkaProducer发送出去的消息先进入到客户端本地的内存缓冲区，然后把很多消息收集成一个一个的Batch，再发送到Broker上去的
        // 如过Kafka发送消息的速度 > 消息发送到Broker的速度,那么消息就会在缓冲区堆积，导致缓冲区不足
        props.put("buffer.memory", 33554432);

        // kv序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 创建生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        // kafka集群中开启consumer进程：bin/kafka-console-consumer.sh --bootstrap-server hadoop101:9092 --topic first
        // consumer消费数据是消费完一个分区后再消费另一个分区
        for (int i = 0; i < 10; i++) {
            kafkaProducer.send(new ProducerRecord<String, String>("first", String.valueOf(i)), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("partitions:" + metadata.partition() + "--" + "offset:" + metadata.offset());
                    } else {
                        System.out.println("发送失败");
                    }
                }
            });
        }

        // 关闭资源
        kafkaProducer.close();
    }
}