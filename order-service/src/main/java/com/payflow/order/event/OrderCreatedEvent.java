package com.payflow.order.event;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

public class OrderCreatedEvent {

    private String eventId;
    private String sagaId;
    private Long orderId;
    private Long customerId;
    private Long productId;
    private Integer quantity;
    private BigDecimal amount;
    private Boolean simulatePaymentFailure;
    private Boolean simulateInventoryFailure;
    private LocalDateTime timestamp;

    public OrderCreatedEvent() {
    }

    public OrderCreatedEvent(Long orderId, Long customerId, BigDecimal amount, Boolean simulatePaymentFailure) {
        this.eventId = UUID.randomUUID().toString();
        this.sagaId = UUID.randomUUID().toString();
        this.orderId = orderId;
        this.customerId = customerId;
        this.productId = 1001L;
        this.quantity = 1;
        this.amount = amount;
        this.simulatePaymentFailure = simulatePaymentFailure != null && simulatePaymentFailure;
        this.simulateInventoryFailure = false;
        this.timestamp = LocalDateTime.now();
    }

    public OrderCreatedEvent(String sagaId, Long orderId, Long customerId, Long productId, Integer quantity,
                             BigDecimal amount, Boolean simulatePaymentFailure, Boolean simulateInventoryFailure) {
        this.eventId = UUID.randomUUID().toString();
        this.sagaId = sagaId;
        this.orderId = orderId;
        this.customerId = customerId;
        this.productId = productId != null ? productId : 1001L;
        this.quantity = quantity != null ? quantity : 1;
        this.amount = amount;
        this.simulatePaymentFailure = simulatePaymentFailure != null && simulatePaymentFailure;
        this.simulateInventoryFailure = simulateInventoryFailure != null && simulateInventoryFailure;
        this.timestamp = LocalDateTime.now();
    }

    public OrderCreatedEvent(String eventId, String sagaId, Long orderId, Long customerId, Long productId,
                             Integer quantity, BigDecimal amount, Boolean simulatePaymentFailure,
                             Boolean simulateInventoryFailure, LocalDateTime timestamp) {
        this.eventId = eventId;
        this.sagaId = sagaId;
        this.orderId = orderId;
        this.customerId = customerId;
        this.productId = productId;
        this.quantity = quantity;
        this.amount = amount;
        this.simulatePaymentFailure = simulatePaymentFailure;
        this.simulateInventoryFailure = simulateInventoryFailure;
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

    public Long getCustomerId() {
        return customerId;
    }

    public void setCustomerId(Long customerId) {
        this.customerId = customerId;
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
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

    public Boolean getSimulateInventoryFailure() {
        return simulateInventoryFailure;
    }

    public void setSimulateInventoryFailure(Boolean simulateInventoryFailure) {
        this.simulateInventoryFailure = simulateInventoryFailure;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }
}
