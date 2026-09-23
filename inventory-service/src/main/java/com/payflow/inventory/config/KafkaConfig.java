package com.payflow.inventory.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import org.springframework.context.annotation.Profile;

@Configuration
@Profile("!test")
public class KafkaConfig {

    @Value("${app.kafka.topics.inventory-command:inventory-command}")
    private String inventoryCommandTopic;

    @Value("${app.kafka.topics.inventory-result:inventory-result}")
    private String inventoryResultTopic;

    @Bean
    public NewTopic inventoryCommandTopic() {
        return TopicBuilder.name(inventoryCommandTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic inventoryResultTopic() {
        return TopicBuilder.name(inventoryResultTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
