package com.payflow.order.kafka;

import com.payflow.order.event.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String orderCreatedTopic;

    public OrderEventProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${app.kafka.topics.order-created:order-created}") String orderCreatedTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.orderCreatedTopic = orderCreatedTopic;
    }

    public void sendOrderCreatedEvent(OrderCreatedEvent event) {
        String key = String.valueOf(event.getOrderId());
        log.info("Publishing OrderCreatedEvent to topic '{}' with key '{}': eventId={}, amount={}, simulatePaymentFailure={}",
                orderCreatedTopic, key, event.getEventId(), event.getAmount(), event.getSimulatePaymentFailure());

        kafkaTemplate.send(orderCreatedTopic, key, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Successfully published OrderCreatedEvent [eventId={}] to partition {} at offset {}",
                                event.getEventId(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("Failed to publish OrderCreatedEvent [eventId={}]: {}",
                                event.getEventId(), ex.getMessage(), ex);
                    }
                });
    }
}
