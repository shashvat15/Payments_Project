package com.payflow.saga.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import org.springframework.context.annotation.Profile;

@Configuration
@Profile("!test")
public class KafkaConfig {

    @Value("${app.kafka.topics.order-created:order-created}")
    private String orderCreatedTopic;

    @Value("${app.kafka.topics.inventory-command:inventory-command}")
    private String inventoryCommandTopic;

    @Value("${app.kafka.topics.inventory-result:inventory-result}")
    private String inventoryResultTopic;

    @Value("${app.kafka.topics.payment-command:payment-command}")
    private String paymentCommandTopic;

    @Value("${app.kafka.topics.payment-result:payment-result}")
    private String paymentResultTopic;

    @Value("${app.kafka.topics.order-command:order-command}")
    private String orderCommandTopic;

    @Bean
    public NewTopic orderCreatedTopic() {
        return TopicBuilder.name(orderCreatedTopic).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic inventoryCommandTopic() {
        return TopicBuilder.name(inventoryCommandTopic).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic inventoryResultTopic() {
        return TopicBuilder.name(inventoryResultTopic).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic paymentCommandTopic() {
        return TopicBuilder.name(paymentCommandTopic).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic paymentResultTopic() {
        return TopicBuilder.name(paymentResultTopic).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic orderCommandTopic() {
        return TopicBuilder.name(orderCommandTopic).partitions(1).replicas(1).build();
    }
}
