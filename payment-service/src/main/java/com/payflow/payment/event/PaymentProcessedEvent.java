package com.payflow.payment.event;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

public class PaymentProcessedEvent {

    private String eventId;
    private String sagaId;
    private Long orderId;
    private Long paymentId;
    private BigDecimal amount;
    private String status; // SUCCESS or FAILED
    private LocalDateTime timestamp;

    public PaymentProcessedEvent() {
    }

    public PaymentProcessedEvent(Long orderId, Long paymentId, BigDecimal amount, String status) {
        this.eventId = UUID.randomUUID().toString();
        this.orderId = orderId;
        this.paymentId = paymentId;
        this.amount = amount;
        this.status = status;
        this.timestamp = LocalDateTime.now();
    }

    public PaymentProcessedEvent(String sagaId, Long orderId, Long paymentId, BigDecimal amount, String status) {
        this.eventId = UUID.randomUUID().toString();
        this.sagaId = sagaId;
        this.orderId = orderId;
        this.paymentId = paymentId;
        this.amount = amount;
        this.status = status;
        this.timestamp = LocalDateTime.now();
    }

    public PaymentProcessedEvent(String eventId, String sagaId, Long orderId, Long paymentId, BigDecimal amount, String status, LocalDateTime timestamp) {
        this.eventId = eventId;
        this.sagaId = sagaId;
        this.orderId = orderId;
        this.paymentId = paymentId;
        this.amount = amount;
        this.status = status;
        this.timestamp = timestamp;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getSagaId() {
        return sagaId;
    }

    public void setSagaId(String sagaId) {
        this.sagaId = sagaId;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public Long getPaymentId() {
        return paymentId;
    }

    public void setPaymentId(Long paymentId) {
        this.paymentId = paymentId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }
}
