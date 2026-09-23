package com.payflow.saga.event;

import java.time.LocalDateTime;

public class InventoryResultEvent {

    private String eventId;
    private String sagaId;
    private Long orderId;
    private Long productId;
    private Integer quantity;
    private String status; // "RESERVED", "RESERVATION_FAILED", "RELEASED"
    private String reason;
    private LocalDateTime timestamp;

    public InventoryResultEvent() {
    }

    public InventoryResultEvent(String eventId, String sagaId, Long orderId, Long productId, Integer quantity, String status, String reason, LocalDateTime timestamp) {
        this.eventId = eventId;
        this.sagaId = sagaId;
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.status = status;
        this.reason = reason;
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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }
}
