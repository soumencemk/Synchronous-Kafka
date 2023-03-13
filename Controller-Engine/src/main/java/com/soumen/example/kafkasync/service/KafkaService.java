package com.soumen.example.kafkasync.service;


import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaService {
    private final ReplyingKafkaTemplate<String, Object, Object> replyingKafkaTemplate;
    @Value("${app.send-topic}")
    private String SEND_TOPIC;

    @SneakyThrows
    public Object kafkaRequestReply(Object request) {
        log.info("Sending Message : {}", request);
        ProducerRecord<String, Object> record = new ProducerRecord<>(SEND_TOPIC, request);
        RequestReplyFuture<String, Object, Object> replyFuture = replyingKafkaTemplate.sendAndReceive(record);
        SendResult<String, Object> sendResult = replyFuture
                .getSendFuture().get(10, TimeUnit.SECONDS);
        ConsumerRecord<String, Object> consumerRecord = replyFuture.get(10, TimeUnit.SECONDS);
        return consumerRecord.value();
    }
}
