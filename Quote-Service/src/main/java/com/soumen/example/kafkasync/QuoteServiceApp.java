package com.soumen.example.kafkasync;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@SpringBootApplication
public class QuoteServiceApp {

    public static void main(String[] args) {
        SpringApplication.run(QuoteServiceApp.class, args);
    }

}


@Component
@RequiredArgsConstructor
@Slf4j
class KafkaConsumer {

    private final KafkaTemplate kafkaTemplate;

    @Value("${app.reply-topic}")
    private String replyTopic;

    @KafkaListener(groupId = "${app.consumer-group}", topics = "${app.send-topic}")
    @SneakyThrows
    public void listen(ConsumerRecord<String, Object> consumerRecord,
                       @Header(KafkaHeaders.CORRELATION_ID) byte[] cor_id,
                       @Header(KafkaHeaders.REPLY_TOPIC) byte[] replyTopic) {
        var value = consumerRecord.value();
        log.info("Message : {}", value);
        sendToReplyTopic(getRandomQuote(), cor_id, replyTopic);

    }

    private void sendToReplyTopic(String randomQuote, byte[] cor_id, byte[] replyTopicHeader) {
        log.info("Publishing to Reply topic ");
        ProducerRecord producerRecord = new ProducerRecord(replyTopic, randomQuote);
        producerRecord.headers().add(KafkaHeaders.CORRELATION_ID, cor_id);
        //producerRecord.headers().add(KafkaHeaders.REPLY_TOPIC, replyTopicHeader);
        kafkaTemplate.send(producerRecord);
    }

    private String getRandomQuote() {
        return "M Gandhi,An eye for an eye will make the whole world blind";
    }
}
