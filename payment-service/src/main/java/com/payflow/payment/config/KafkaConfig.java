package com.payflow.payment.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import org.springframework.context.annotation.Profile;

@Configuration
@Profile("!test")
public class KafkaConfig {

    @Value("${app.kafka.topics.payment-command:payment-command}")
    private String paymentCommandTopic;

    @Value("${app.kafka.topics.payment-result:payment-result}")
    private String paymentResultTopic;

    @Bean
    public NewTopic paymentCommandTopic() {
        return TopicBuilder.name(paymentCommandTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic paymentResultTopic() {
        return TopicBuilder.name(paymentResultTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
