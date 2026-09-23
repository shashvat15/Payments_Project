package com.payflow.inventory.event;

import java.time.LocalDateTime;
import java.util.UUID;

public class InventoryReleasedEvent {

    private String eventId;
    private String sagaId;
    private Long orderId;
    private Long productId;
    private Integer quantity;
    private String status = "RELEASED";
    private LocalDateTime timestamp;

    public InventoryReleasedEvent() {
    }

    public InventoryReleasedEvent(String sagaId, Long orderId, Long productId, Integer quantity) {
        this.eventId = UUID.randomUUID().toString();
        this.sagaId = sagaId;
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.status = "RELEASED";
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

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }
}
