package com.soumen.example.kafkasync.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

@Configuration
public class KafkaConfig {
    @Value("${app.reply-topic}")
    private String REPLY_TOPICS;
    @Value("${app.consumer-group}")
    private String CONSUMER_GROUPS;

    @Bean
    public ReplyingKafkaTemplate<String, Object, Object>
    replyingKafkaTemplate(ProducerFactory<String, Object> producerFactory,
                          ConcurrentMessageListenerContainer<String, Object> repliesContainer) {
        return new ReplyingKafkaTemplate<>(producerFactory, repliesContainer);

    }

    @Bean
    ConcurrentMessageListenerContainer<String, Object> repliesContainer(
            ConcurrentKafkaListenerContainerFactory<String, Object> factory
    ) {
        var container = factory.createContainer(REPLY_TOPICS);
        container.getContainerProperties().setGroupId(CONSUMER_GROUPS);
        container.setAutoStartup(false);
        return container;
    }
}
