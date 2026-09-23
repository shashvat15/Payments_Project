package com.payflow.payment.kafka;

import com.payflow.payment.event.OrderCreatedEvent;
import com.payflow.payment.event.PaymentProcessedEvent;
import com.payflow.payment.service.PaymentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    private final PaymentService paymentService;
    private final PaymentEventProducer paymentEventProducer;

    public OrderEventConsumer(PaymentService paymentService, PaymentEventProducer paymentEventProducer) {
        this.paymentService = paymentService;
        this.paymentEventProducer = paymentEventProducer;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.order-created:order-created}",
            groupId = "${spring.kafka.consumer.group-id:payment-service-group}"
    )
    public void consumeOrderCreatedEvent(OrderCreatedEvent event) {
        log.info("Received OrderCreatedEvent from Kafka: eventId={}, orderId={}, customerId={}, amount={}, simulatePaymentFailure={}",
                event.getEventId(), event.getOrderId(), event.getCustomerId(), event.getAmount(), event.getSimulatePaymentFailure());

        try {
            PaymentProcessedEvent processedEvent = paymentService.processPaymentFromEvent(event);
            paymentEventProducer.sendPaymentProcessedEvent(processedEvent);
            log.info("Successfully processed payment and published PaymentProcessedEvent for orderId={}", event.getOrderId());
        } catch (Exception ex) {
            log.error("Failed to process payment from OrderCreatedEvent for orderId={}: {}",
                    event.getOrderId(), ex.getMessage(), ex);
            throw ex;
        }
    }
}
