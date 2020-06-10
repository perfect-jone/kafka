package com.bigdata.kafka.consumer;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

//根据指定的Topics、Partition、Offset来获取数据
public class LowerConsumer {
    public static void main(String[] args) {

        // 定义相关参数
        // Kafka集群节点
        ArrayList<String> brokers = new ArrayList<String>();
        brokers.add("hadoop101");
        brokers.add("hadoop102");
        brokers.add("hadoop103");
        // 端口号
        int port = 9092;
        // Topics
        String topic = "first";
        // Partition
        int partition = 0;
        // Offset
        long offset = 2L;
        LowerConsumer lowerConsumer = new LowerConsumer();
        lowerConsumer.getData(brokers, port, topic, partition, offset);
    }

    // 找分区leader
    private BrokerEndPoint findLeader(ArrayList<String> brokers, int port, String topic, int partition) {

        for (String broker : brokers) {

            // 创建获取分区leader的消费者对象
            SimpleConsumer findLeader = new SimpleConsumer(broker, port, 1000, 1024 * 4, "findLeader");

            // 创建一个Topics元数据信息请求
            TopicMetadataResponse metadataResponse = findLeader.send(new TopicMetadataRequest(Collections.singletonList(topic)));

            // 响应Topics元数据信息请求
            List<TopicMetadata> topicsMetadata = metadataResponse.topicsMetadata();

            // 遍历Topics元数据信息
            for (TopicMetadata topicMetadata : topicsMetadata) {

                // Topics元数据信息获取Partitions元数据信息，因为这里面有Leader信息
                List<PartitionMetadata> partitionsMetadata = topicMetadata.partitionsMetadata();

                // 遍历Partitions元数据信息
                for (PartitionMetadata partitionMetadatum : partitionsMetadata) {

                    // 通过Partitions元数据信息得到Leader
                    // 只获取partition=0分区的数据
                    if (partition == partitionMetadatum.partitionId()) {
                        return partitionMetadatum.leader();
                    }
                }
            }
        }
        // Kafka集群宕机才返回null
        return null;
    }

    // 获取数据
    private void getData(ArrayList<String> brokers, int port, String topic, int partition, long offset) {

        // 获取分区Leader
        BrokerEndPoint leader = findLeader(brokers, port, topic, partition);
        if (leader == null) {
            return;
        }

        // 创建获取数据的消费者对象
        SimpleConsumer getData = new SimpleConsumer(leader.host(), port, 1000, 1024 * 4, "getData");

        // 创建FetchRequest对象
        FetchRequest fetchRequest = new FetchRequestBuilder().addFetch(topic, partition, offset, 1024).build();

        // 获取FetchResponse对象
        FetchResponse fetchResponse = getData.fetch(fetchRequest);

        // 解析FetchResponse对象
        ByteBufferMessageSet messageAndOffsets = fetchResponse.messageSet(topic, partition);
        for (MessageAndOffset messageAndOffset : messageAndOffsets) {
            long offset1 = messageAndOffset.offset();
            ByteBuffer buffer = messageAndOffset.message().payload();
            byte[] bytes = new byte[buffer.limit()];
            buffer.get(bytes);
            System.out.println("offset:" + offset1 + "\t" + "message:" + new String(bytes));
        }
    }
}
