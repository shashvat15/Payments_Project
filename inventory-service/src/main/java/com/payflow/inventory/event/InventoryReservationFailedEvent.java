package com.payflow.inventory.event;

import java.time.LocalDateTime;
import java.util.UUID;

public class InventoryReservationFailedEvent {

    private String eventId;
    private String sagaId;
    private Long orderId;
    private Long productId;
    private Integer quantity;
    private String status = "RESERVATION_FAILED";
    private String reason;
    private LocalDateTime timestamp;

    public InventoryReservationFailedEvent() {
    }

    public InventoryReservationFailedEvent(String sagaId, Long orderId, Long productId, Integer quantity, String reason) {
        this.eventId = UUID.randomUUID().toString();
        this.sagaId = sagaId;
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.status = "RESERVATION_FAILED";
        this.reason = reason;
        this.timestamp = LocalDateTime.now();
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
