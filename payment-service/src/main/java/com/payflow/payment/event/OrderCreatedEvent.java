package com.payflow.payment.event;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public class OrderCreatedEvent {

    private String eventId;
    private Long orderId;
    private Long customerId;
    private BigDecimal amount;
    private Boolean simulatePaymentFailure;
    private LocalDateTime timestamp;

    public OrderCreatedEvent() {
    }

    public OrderCreatedEvent(String eventId, Long orderId, Long customerId, BigDecimal amount, Boolean simulatePaymentFailure, LocalDateTime timestamp) {
        this.eventId = eventId;
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.simulatePaymentFailure = simulatePaymentFailure;
        this.timestamp = timestamp;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public Long getCustomerId() {
        return customerId;
    }

    public void setCustomerId(Long customerId) {
        this.customerId = customerId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public Boolean getSimulatePaymentFailure() {
        return simulatePaymentFailure;
    }

    public void setSimulatePaymentFailure(Boolean simulatePaymentFailure) {
        this.simulatePaymentFailure = simulatePaymentFailure;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }
}
