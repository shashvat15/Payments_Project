package com.payflow.order.kafka;

import com.payflow.order.event.PaymentProcessedEvent;
import com.payflow.order.service.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventConsumer.class);

    private final OrderService orderService;

    public PaymentEventConsumer(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.payment-processed:payment-processed}",
            groupId = "${spring.kafka.consumer.group-id:order-service-group}"
    )
    public void consumePaymentProcessedEvent(PaymentProcessedEvent event) {
        log.info("Received PaymentProcessedEvent from Kafka: eventId={}, orderId={}, paymentId={}, status={}",
                event.getEventId(), event.getOrderId(), event.getPaymentId(), event.getStatus());

        try {
            orderService.handlePaymentProcessedEvent(event);
            log.info("Successfully handled PaymentProcessedEvent for orderId={}", event.getOrderId());
        } catch (Exception ex) {
            log.error("Error while processing PaymentProcessedEvent for orderId={}: {}",
                    event.getOrderId(), ex.getMessage(), ex);
            // In Phase 2, unhandled exceptions will be retried by Kafka default error handler
            throw ex;
        }
    }
}
