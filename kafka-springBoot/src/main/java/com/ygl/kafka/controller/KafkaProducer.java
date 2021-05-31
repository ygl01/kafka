// Copyright (C) 2021 Focus Media Holding Ltd. All Rights Reserved.

package com.ygl.kafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * TODO
 * ��������
 *
 * @author ygl
 * @version V1.0
 * @since 2021-05-28 16:28
 **/
@RestController
@RequestMapping("/kafka")
public class KafkaProducer {

    public static final String TOPIC1 = "topic1";

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    //������Ϣ
    @GetMapping("/sendMessage1/{message}")
    public void sendMessage1(@PathVariable("message") String message) {

        kafkaTemplate.send(TOPIC1, message);
    }

    //������Ϣ(���ص���������)
    @GetMapping("/addCallbackOne/{message}")
    public void addCallbackOne(@PathVariable("message") String message) {

        kafkaTemplate.send(TOPIC1, message).addCallback(success -> {
            System.out.println("������Ϣ�ɹ���");
        }, failure -> {
            System.out.println("������Ϣʧ�ܣ�");
        });
    }

    //������Ϣ���ڶ��ִ��ص������ߣ�
    @GetMapping("/addCallbackTwo/{message}")
    public void addCallbackTwo(@PathVariable("message") String message) {

        kafkaTemplate.send(TOPIC1, message).addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
            @Override
            public void onFailure(Throwable throwable) {

                System.out.println("������Ϣʧ�ܣ�");
            }

            @Override
            public void onSuccess(SendResult<String, Object> result) {

                String topic = result.getRecordMetadata().topic();
                int partition = result.getRecordMetadata().partition();
                long offset = result.getRecordMetadata().offset();
                System.out.println("������Ϣ�ɹ���" + "topic��" + topic + "��partition:" + partition + "��offset��" + offset);
            }
        });
    }

    /**
     * TODO kafka�ֲ�ʽ����
     *
     * @return null
     * @author ygl
     * @date 2021/5/31 11:31
     **/
    @GetMapping("/kafka/transaction")
    public void sendMessageThree() {

        //�������񣬺��汨��ǰ����Ϣ���ᷢ�ͳ�ȥ
        kafkaTemplate.executeInTransaction(operations -> {
            operations.send(KafkaProducer.TOPIC1, "test executeInTransaction");
            throw new RuntimeException("fain");
        });

        //���������񣬺��汨��ǰ����Ϣ�Ѿ����ͳɹ���
        kafkaTemplate.send(KafkaProducer.TOPIC1,"test executeInTransaction");
        throw new RuntimeException("fail");
    }

}
