package com.payflow.order.kafka;

import com.payflow.order.event.PaymentProcessedEvent;
import com.payflow.order.service.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Phase 2A Choreography Consumer (Deprecated in Phase 2B).
 * In Phase 2B Orchestration, OrderService no longer listens directly to 'payment-processed'.
 * Instead, the Saga Orchestrator manages order lifecycle and issues 'ConfirmOrder' or 'CancelOrder'
 * commands to 'order-command', consumed by OrderCommandConsumer.
 */
@Deprecated
public class PaymentEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventConsumer.class);

    private final OrderService orderService;

    public PaymentEventConsumer(OrderService orderService) {
        this.orderService = orderService;
    }

    public void consumePaymentProcessedEvent(PaymentProcessedEvent event) {
        log.info("[Deprecated Phase 2A] Received PaymentProcessedEvent: eventId={}, orderId={}", event.getEventId(), event.getOrderId());
        orderService.handlePaymentProcessedEvent(event);
    }
}
