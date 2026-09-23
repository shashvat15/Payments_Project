package com.payflow.saga.kafka;

import com.payflow.saga.event.InventoryResultEvent;
import com.payflow.saga.event.OrderCreatedEvent;
import com.payflow.saga.event.PaymentProcessedEvent;
import com.payflow.saga.service.SagaOrchestratorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SagaEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(SagaEventConsumer.class);

    private final SagaOrchestratorService sagaOrchestratorService;

    public SagaEventConsumer(SagaOrchestratorService sagaOrchestratorService) {
        this.sagaOrchestratorService = sagaOrchestratorService;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.order-created:order-created}",
            groupId = "${spring.kafka.consumer.group-id:saga-orchestrator-group}"
    )
    public void consumeOrderCreatedEvent(OrderCreatedEvent event) {
        log.info("Saga Orchestrator consumed OrderCreatedEvent: sagaId={}, orderId={}, customerId={}, amount={}",
                event.getSagaId(), event.getOrderId(), event.getCustomerId(), event.getAmount());
        try {
            sagaOrchestratorService.handleOrderCreated(event);
        } catch (Exception ex) {
            log.error("Error processing OrderCreatedEvent in Saga Orchestrator: {}", ex.getMessage(), ex);
            throw ex;
        }
    }

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-result:inventory-result}",
            groupId = "${spring.kafka.consumer.group-id:saga-orchestrator-group}"
    )
    public void consumeInventoryResultEvent(InventoryResultEvent event) {
        log.info("Saga Orchestrator consumed InventoryResultEvent: sagaId={}, status={}, orderId={}, productId={}",
                event.getSagaId(), event.getStatus(), event.getOrderId(), event.getProductId());
        try {
            sagaOrchestratorService.handleInventoryResult(event);
        } catch (Exception ex) {
            log.error("Error processing InventoryResultEvent in Saga Orchestrator: {}", ex.getMessage(), ex);
            throw ex;
        }
    }

    @KafkaListener(
            topics = "${app.kafka.topics.payment-result:payment-result}",
            groupId = "${spring.kafka.consumer.group-id:saga-orchestrator-group}"
    )
    public void consumePaymentResultEvent(PaymentProcessedEvent event) {
        log.info("Saga Orchestrator consumed PaymentProcessedEvent: sagaId={}, status={}, orderId={}, paymentId={}",
                event.getSagaId(), event.getStatus(), event.getOrderId(), event.getPaymentId());
        try {
            sagaOrchestratorService.handlePaymentResult(event);
        } catch (Exception ex) {
            log.error("Error processing PaymentProcessedEvent in Saga Orchestrator: {}", ex.getMessage(), ex);
            throw ex;
        }
    }
}
