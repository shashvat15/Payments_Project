package com.payflow.inventory.kafka;

import com.payflow.inventory.event.InventoryResultEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class InventoryEventProducer {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String inventoryResultTopic;

    public InventoryEventProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${app.kafka.topics.inventory-result:inventory-result}") String inventoryResultTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.inventoryResultTopic = inventoryResultTopic;
    }

    public CompletableFuture<SendResult<String, Object>> sendInventoryResultEvent(InventoryResultEvent event) {
        String key = String.valueOf(event.getOrderId());
        log.info("Publishing InventoryResultEvent to topic '{}' with key '{}': sagaId={}, eventId={}, status={}",
                inventoryResultTopic, key, event.getSagaId(), event.getEventId(), event.getStatus());

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(inventoryResultTopic, key, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Successfully published InventoryResultEvent [eventId={}, sagaId={}] to partition {} at offset {}",
                        event.getEventId(), event.getSagaId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to publish InventoryResultEvent [eventId={}, sagaId={}]: {}",
                        event.getEventId(), event.getSagaId(), ex.getMessage(), ex);
            }
        });

        return future;
    }
}
