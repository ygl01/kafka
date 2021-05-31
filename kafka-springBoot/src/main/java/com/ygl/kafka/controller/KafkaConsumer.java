// Copyright (C) 2021 Focus Media Holding Ltd. All Rights Reserved.

package com.ygl.kafka.controller;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

/**
 * TODO
 * �������ߣ����м���
 *
 * @author ygl
 * @version V1.0
 * @since 2021-05-28 16:32
 **/
@Component
public class KafkaConsumer {

    @KafkaListener(topics = KafkaProducer.TOPIC1)
    public void onMessage1(ConsumerRecord<?, ?> record) {

        //�����Ǹ�topic��partition����Ϣ����ӡ����Ϣ����
        System.out.println("�����ѣ�" + record.topic() + "-" + record.partition() + "-" + record.value());
    }
    /**
    *
    * TODO
     * ָ��topic��partition��offset����
     * ͬʱ����topic1��topic2������topic1��0�ŷ�����topic2�� "0�ź�1��" ������ָ��1�ŷ�����offset��ʼֵΪ8
     *
     * �� id��������ID��
     *
     * �� groupId��������ID��
     *
     * �� topics��������topic���ɼ��������
     *
     * �� topicPartitions�������ø�����ϸ�ļ�����Ϣ����ָ��topic��parition��offset������
     *
     * ע�⣺topics��topicPartitions����ͬʱʹ�ã�
    * @return null
    * @author ygl
    * @date 2021/5/28 18:36
    **/
    @KafkaListener(id = "consumer1",groupId = "felix-group",topicPartitions = {
            @TopicPartition(topic = KafkaProducer.TOPIC1,partitions = {"0"}),
            @TopicPartition(topic = "topic2",partitions = "0",partitionOffsets = @PartitionOffset(partition = "1",initialOffset = "2"))
    })
    public void onMessage2(ConsumerRecord<?, ?> record) {

        //�����Ǹ�topic��partition����Ϣ����ӡ����Ϣ����
        System.out.println("������2��" + record.topic() + "-" + record.partition() + "-" + record.value());
    }



}
