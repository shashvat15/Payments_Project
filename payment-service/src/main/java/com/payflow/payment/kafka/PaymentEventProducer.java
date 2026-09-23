package com.payflow.payment.kafka;

import com.payflow.payment.event.PaymentProcessedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventProducer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String paymentProcessedTopic;

    public PaymentEventProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${app.kafka.topics.payment-processed:payment-processed}") String paymentProcessedTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.paymentProcessedTopic = paymentProcessedTopic;
    }

    public void sendPaymentProcessedEvent(PaymentProcessedEvent event) {
        String key = String.valueOf(event.getOrderId());
        log.info("Publishing PaymentProcessedEvent to topic '{}' with key '{}': eventId={}, paymentId={}, status={}",
                paymentProcessedTopic, key, event.getEventId(), event.getPaymentId(), event.getStatus());

        kafkaTemplate.send(paymentProcessedTopic, key, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Successfully published PaymentProcessedEvent [eventId={}] to partition {} at offset {}",
                                event.getEventId(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("Failed to publish PaymentProcessedEvent [eventId={}]: {}",
                                event.getEventId(), ex.getMessage(), ex);
                    }
                });
    }
}
