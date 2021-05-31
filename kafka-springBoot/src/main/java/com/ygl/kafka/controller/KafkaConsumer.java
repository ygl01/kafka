// Copyright (C) 2021 Focus Media Holding Ltd. All Rights Reserved.

package com.ygl.kafka.controller;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

/**
 * TODO
 * 简单消费者，进行监听
 *
 * @author ygl
 * @version V1.0
 * @since 2021-05-28 16:32
 **/
@Component
public class KafkaConsumer {

    @KafkaListener(topics = KafkaProducer.TOPIC1)
    public void onMessage1(ConsumerRecord<?, ?> record) {

        //消费那个topic、partition的消息，打印出消息内容
        System.out.println("简单消费：" + record.topic() + "-" + record.partition() + "-" + record.value());
    }
    /**
    *
    * TODO
     * 指定topic、partition、offset消费
     * 同时监听topic1和topic2，监听topic1的0号分区、topic2的 "0号和1号" 分区，指向1号分区的offset初始值为8
     *
     * ① id：消费者ID；
     *
     * ② groupId：消费组ID；
     *
     * ③ topics：监听的topic，可监听多个；
     *
     * ④ topicPartitions：可配置更加详细的监听信息，可指定topic、parition、offset监听。
     *
     * 注意：topics和topicPartitions不能同时使用；
    * @return null
    * @author ygl
    * @date 2021/5/28 18:36
    **/
    @KafkaListener(id = "consumer1",groupId = "felix-group",topicPartitions = {
            @TopicPartition(topic = KafkaProducer.TOPIC1,partitions = {"0"}),
            @TopicPartition(topic = "topic2",partitions = "0",partitionOffsets = @PartitionOffset(partition = "1",initialOffset = "2"))
    })
    public void onMessage2(ConsumerRecord<?, ?> record) {

        //消费那个topic、partition的消息，打印出消息内容
        System.out.println("简单消费2：" + record.topic() + "-" + record.partition() + "-" + record.value());
    }



}
