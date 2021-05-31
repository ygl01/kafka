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
 * 简单生产者
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

    //发送消息
    @GetMapping("/sendMessage1/{message}")
    public void sendMessage1(@PathVariable("message") String message) {

        kafkaTemplate.send(TOPIC1, message);
    }

    //发送消息(带回调的生产者)
    @GetMapping("/addCallbackOne/{message}")
    public void addCallbackOne(@PathVariable("message") String message) {

        kafkaTemplate.send(TOPIC1, message).addCallback(success -> {
            System.out.println("发送消息成功！");
        }, failure -> {
            System.out.println("发送消息失败！");
        });
    }

    //发送消息（第二种带回调生产者）
    @GetMapping("/addCallbackTwo/{message}")
    public void addCallbackTwo(@PathVariable("message") String message) {

        kafkaTemplate.send(TOPIC1, message).addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
            @Override
            public void onFailure(Throwable throwable) {

                System.out.println("发送消息失败！");
            }

            @Override
            public void onSuccess(SendResult<String, Object> result) {

                String topic = result.getRecordMetadata().topic();
                int partition = result.getRecordMetadata().partition();
                long offset = result.getRecordMetadata().offset();
                System.out.println("发送消息成功！" + "topic：" + topic + "；partition:" + partition + "；offset：" + offset);
            }
        });
    }

    /**
     * TODO kafka分布式事务
     *
     * @return null
     * @author ygl
     * @date 2021/5/31 11:31
     **/
    @GetMapping("/kafka/transaction")
    public void sendMessageThree() {

        //声明事务，后面报错，前面消息不会发送出去
        kafkaTemplate.executeInTransaction(operations -> {
            operations.send(KafkaProducer.TOPIC1, "test executeInTransaction");
            throw new RuntimeException("fain");
        });

        //不声明事务，后面报错但前面消息已经发送成功了
        kafkaTemplate.send(KafkaProducer.TOPIC1,"test executeInTransaction");
        throw new RuntimeException("fail");
    }

}
