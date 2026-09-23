package com.payflow.payment.kafka;

import com.payflow.payment.event.OrderCreatedEvent;
import com.payflow.payment.event.PaymentProcessedEvent;
import com.payflow.payment.service.PaymentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Phase 2A Choreography Consumer (Deprecated in Phase 2B).
 * In Phase 2B Orchestration, PaymentService no longer listens directly to 'order-created'.
 * Instead, the Saga Orchestrator dispatches 'ProcessPaymentCommand' to 'payment-command',
 * which is consumed by PaymentCommandConsumer.
 */
@Deprecated
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    private final PaymentService paymentService;
    private final PaymentEventProducer paymentEventProducer;

    public OrderEventConsumer(PaymentService paymentService, PaymentEventProducer paymentEventProducer) {
        this.paymentService = paymentService;
        this.paymentEventProducer = paymentEventProducer;
    }

    public void consumeOrderCreatedEvent(OrderCreatedEvent event) {
        log.info("[Deprecated Phase 2A] Received OrderCreatedEvent: eventId={}, orderId={}", event.getEventId(), event.getOrderId());
        PaymentProcessedEvent processedEvent = paymentService.processPaymentFromEvent(event);
        paymentEventProducer.sendPaymentProcessedEvent(processedEvent);
    }
}
