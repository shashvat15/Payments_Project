package com.payflow.payment.kafka;

import com.payflow.payment.event.PaymentProcessedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class PaymentEventProducer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String paymentResultTopic;

    public PaymentEventProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${app.kafka.topics.payment-result:payment-result}") String paymentResultTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.paymentResultTopic = paymentResultTopic;
    }

    public CompletableFuture<SendResult<String, Object>> sendPaymentProcessedEvent(PaymentProcessedEvent event) {
        String key = String.valueOf(event.getOrderId());
        log.info("Publishing PaymentProcessedEvent to topic '{}' with key '{}': sagaId={}, eventId={}, paymentId={}, status={}",
                paymentResultTopic, key, event.getSagaId(), event.getEventId(), event.getPaymentId(), event.getStatus());

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(paymentResultTopic, key, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Successfully published PaymentProcessedEvent [eventId={}, sagaId={}] to partition {} at offset {}",
                        event.getEventId(),
                        event.getSagaId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to publish PaymentProcessedEvent [eventId={}, sagaId={}]: {}",
                        event.getEventId(), event.getSagaId(), ex.getMessage(), ex);
            }
        });

        return future;
    }
}
